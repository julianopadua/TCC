# src/modeling_build_datasets.py
# =============================================================================
# CONSTRUCAO DE BASES PARA MODELAGEM (INMET + BDQueimadas, CERRADO)
# =============================================================================
# Objetivo:
# - Carregar os CSVs anuais consolidados inmet_bdq_{ANO}_cerrado.csv.
# - Harmonizar colunas (especialmente radiacao global).
# - Aplicar regra de missing (NaN + -999/-9999) ano a ano.
# - Gerar 6 bases em data/modeling, em formato parquet, PARTICIONADAS POR ANO:
#
#   Para cada ano, salvamos:
#
#   data/modeling/base_F_full_original/inmet_bdq_{ANO}_cerrado.parquet
#   data/modeling/base_A_no_rad/inmet_bdq_{ANO}_cerrado.parquet
#   data/modeling/base_B_no_rad_knn/inmet_bdq_{ANO}_cerrado.parquet
#   data/modeling/base_C_no_rad_drop_rows/inmet_bdq_{ANO}_cerrado.parquet
#   data/modeling/base_D_with_rad_drop_rows/inmet_bdq_{ANO}_cerrado.parquet
#   data/modeling/base_E_with_rad_knn/inmet_bdq_{ANO}_cerrado.parquet
#
#   1) base_F_full_original
#      - Mantem radiacao global
#      - Nao faz imputacao
#      - Nao remove linhas (apenas trata sentinelas como NaN)
#
#   2) base_A_no_rad
#      - Remove coluna de radiacao global
#      - Nao faz imputacao
#      - Nao remove linhas
#
#   3) base_B_no_rad_knn
#      - Remove radiacao global
#      - Imputa features numericas com KNNImputer (ano a ano)
#
#   4) base_C_no_rad_drop_rows
#      - Remove radiacao global
#      - Remove linhas que tenham qualquer feature faltante
#
#   5) base_D_with_rad_drop_rows
#      - Mantem radiacao global
#      - Remove linhas que tenham qualquer feature faltante
#
#   6) base_E_with_rad_knn
#      - Mantem radiacao global
#      - Imputa features numericas com KNNImputer (ano a ano)
#
# - "Features" aqui = colunas numericas de contexto, exceto:
#   * colunas em EXCLUDE_NON_NUMERIC (datas, cidade, etc.)
#   * colunas alvo (RISCO_FOGO, FRP, FOCO_ID)
#   * label HAS_FOCO
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

import pandas as pd
from sklearn.impute import KNNImputer

from utils import loadConfig, get_logger, get_path, ensure_dir

import time
import numpy as np

try:
    import psutil
except Exception:
    psutil = None


# -----------------------------------------------------------------------------
# CONFIG, LOG E CONSTANTES
# -----------------------------------------------------------------------------
cfg = loadConfig()

# Raiz do projeto
try:
    PROJECT_ROOT: Path = get_path("paths", "root")
except Exception:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Diretório dos CSVs consolidados (dataset)
try:
    DATASET_DIR: Path = get_path("paths", "data", "dataset")
except Exception:
    DATASET_DIR = (PROJECT_ROOT / "data" / "dataset").resolve()

# Diretório base de modeling em data/
try:
    MODELING_DIR: Path = get_path("paths", "data", "modeling")
    MODELING_DIR = ensure_dir(MODELING_DIR)
except Exception:
    MODELING_DIR = ensure_dir(PROJECT_ROOT / "data" / "modeling")

# Logger dedicado
log = get_logger("modeling.build_datasets", kind="modeling", per_run_file=True)

# Padrão dos arquivos ano a ano
FILENAME_PATTERN = "inmet_bdq_*_cerrado.csv"

# Códigos especiais tratados como faltantes
MISSING_CODES = {-999, -9999}
MISSING_CODES_STR = {str(v) for v in MISSING_CODES}

# Colunas que nao entram como features (datas, IDs, texto, etc.)
EXCLUDE_NON_NUMERIC = {
    "DATA (YYYY-MM-DD)",
    "HORA (UTC)",
    "CIDADE",
    "cidade_norm",
    "ts_hour",
    "ANO",  # ANO é contexto, mas não entra no KNN das features
}

# Colunas alvo (labels auxiliares)
TARGET_COLS = {
    "RISCO_FOGO",
    "FRP",
    "FOCO_ID",
}

LABEL_COL = "HAS_FOCO"

# Coluna de radiacao (ja harmonizada)
RADIACAO_COL = "RADIACAO GLOBAL (KJ/m²)"

# Validacao imediata
if not DATASET_DIR.exists():
    raise FileNotFoundError(
        f"Diretorio de datasets nao encontrado.\nTentado: {DATASET_DIR}"
    )

# --- Cenários disponíveis e seus nomes de pasta ---
SCENARIO_MAP = {
    "A": "base_A_no_rad",
    "B": "base_B_no_rad_knn",
    "C": "base_C_no_rad_drop_rows",
    "D": "base_D_with_rad_drop_rows",
    "E": "base_E_with_rad_knn",
    "F": "base_F_full_original",
}
# Ordem canônica para execução
SCENARIO_ORDER = ["F", "A", "B", "C", "D", "E"]

def _fmt_bytes(n: float) -> str:
    for unit in ["B","KB","MB","GB","TB"]:
        if n < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"

def _mem_info() -> str:
    if psutil is None:
        return "psutil indisponível"
    p = psutil.Process()
    rss = p.memory_info().rss
    vm = psutil.virtual_memory()
    return f"RSS={_fmt_bytes(rss)} | RAM livre={_fmt_bytes(vm.available)}"

# -----------------------------------------------------------------------------
# CLASSE PRINCIPAL
# -----------------------------------------------------------------------------
@dataclass
class ModelingDatasetBuilder:
    dataset_dir: Path
    modeling_dir: Path
    file_pattern: str = FILENAME_PATTERN
    missing_codes: set[int] = field(default_factory=lambda: MISSING_CODES.copy())
    exclude_non_numeric: set[str] = field(default_factory=lambda: EXCLUDE_NON_NUMERIC.copy())
    target_cols: set[str] = field(default_factory=lambda: TARGET_COLS.copy())
    label_col: str = LABEL_COL
    radiacao_col: str = RADIACAO_COL

    overwrite_existing: bool = False
    enabled_scenarios: set[str] = field(default_factory=lambda: set(SCENARIO_ORDER))

    # ---- KNN: performance & logging ----
    imputer_chunk_rows: int = 50_000          # tamanho do bloco no transform
    log_heartbeat_sec: int = 20               # frequência dos heartbeats

    # ---- KNN: otimizações simples mas efetivas ----
    imputer_fit_max_rows: Optional[int] = 80_000  # treine o imputer em no máx. M linhas; None/0 = usa tudo
    imputer_prefer_complete_fit: bool = True      # prioriza linhas com menos NaNs no FIT
    imputer_fit_max_missing_frac: float = 0.40    # ao priorizar "completas": máximo % de NaN por linha (0.40 = 40%)
    imputer_weights: str = "uniform"              # 'uniform' ou 'distance'

    # ---- KNN: opcionalmente imputar por mês (reduz N por grupo) ----
    imputer_group_by_month: bool = False
    imputer_min_group_rows: int = 40_000          # grupo < este tamanho cai em fallback (ano inteiro)

    def __post_init__(self) -> None:
        pass

    # ------------------------------
    # Descoberta de arquivos
    # ------------------------------
    def discover_year_files(self) -> Dict[int, Path]:
        mapping: Dict[int, Path] = {}
        for fp in self.dataset_dir.glob(self.file_pattern):
            digits = "".join(ch if ch.isdigit() else " " for ch in fp.stem).split()
            year = None
            for token in digits:
                if len(token) == 4:
                    try:
                        y = int(token)
                    except ValueError:
                        continue
                    if 1900 <= y <= 2100:
                        year = y
                        break
            if year is not None:
                mapping[year] = fp

        mapping = dict(sorted(mapping.items()))
        if not mapping:
            raise FileNotFoundError(
                f"Nenhum CSV encontrado em {self.dataset_dir} com padrao {self.file_pattern}"
            )
        log.info(f"[DISCOVER] {len(mapping)} arquivos anuais detectados.")
        return mapping

    def is_enabled(self, code: str) -> bool:
        return code in self.enabled_scenarios

    def scenario_path_exists(self, code: str, year: int) -> bool:
        return self.get_scenario_path(SCENARIO_MAP[code], year).exists()

    # ------------------------------
    # Harmonização / leitura
    # ------------------------------
    def harmonize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        old = "RADIACAO GLOBAL (Kj/m²)"
        new = self.radiacao_col

        if old in df.columns and new in df.columns:
            filled = df[new].combine_first(df[old])
            conflict_mask = df[old].notna() & df[new].notna() & (df[old] != df[new])
            if conflict_mask.any():
                log.warning(
                    f"[HARMONIZE] Conflitos entre '{old}' e '{new}' em "
                    f"{int(conflict_mask.sum())} linhas; mantendo '{new}'."
                )
            df[new] = filled
            df = df.drop(columns=[old])
        elif old in df.columns:
            df = df.rename(columns={old: new})
        return df

    def read_year_csv(self, fp: Path, year: int) -> pd.DataFrame:
        df = pd.read_csv(fp, sep=",", decimal=",", encoding="utf-8", low_memory=False)
        df = self.harmonize_columns(df)
        if "ANO" not in df.columns:
            df["ANO"] = year
        return df

    # ------------------------------
    # Missing semantics / coerção
    # ------------------------------
    def apply_missing_semantics(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for c in df.columns:
            s = df[c]
            if pd.api.types.is_numeric_dtype(s):
                df[c] = s.replace(list(self.missing_codes), pd.NA)
            else:
                s_str = s.astype("string")
                df[c] = s_str.mask(s_str.str.strip().eq(""))
        return df

    def coerce_feature_columns_to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        feature_cols = self.get_feature_columns(df)
        for c in feature_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("float32")
        n_num = sum(pd.api.types.is_numeric_dtype(df[c]) for c in feature_cols)
        log.info(f"[COERCE] {len(feature_cols)} features candidatas; {n_num} numéricas (float32).")
        return df

    # ------------------------------
    # Seleção de features
    # ------------------------------
    def get_feature_columns(self, df: pd.DataFrame) -> List[str]:
        feature_cols: List[str] = []
        for c in df.columns:
            if c in self.target_cols:
                continue
            if c == self.label_col:
                continue
            if c in self.exclude_non_numeric:
                continue
            feature_cols.append(c)
        log.info(f"[FEATURES] {len(feature_cols)} colunas de feature: {feature_cols}")
        return feature_cols

    def get_numeric_feature_columns(self, df: pd.DataFrame, feature_cols: List[str]) -> List[str]:
        num_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(df[c])]
        log.info(f"[NUM FEATURES] {len(num_cols)} numéricas para KNNImputer: {num_cols}")
        return num_cols

    # ------------------------------
    # KNN: helpers
    # ------------------------------
    def _choose_fit_indices(
        self,
        X: np.ndarray,
        df_block: pd.DataFrame,
        max_rows: int,
        prefer_complete: bool,
        max_missing_frac: float,
    ) -> np.ndarray:
        """
        Escolhe até max_rows índices para FIT, preferindo linhas com menos NaN e
        estratificando por mês quando possível.
        """
        n_rows, n_cols = X.shape
        rng = np.random.default_rng(42)
        if max_rows <= 0 or max_rows >= n_rows:
            return np.arange(n_rows)

        # prioridade: linhas com proporção de NaN <= max_missing_frac
        if prefer_complete:
            nan_per_row = np.isnan(X).sum(axis=1)
            ok_mask = nan_per_row <= int(max_missing_frac * n_cols)
        else:
            ok_mask = np.ones(n_rows, dtype=bool)

        idx_pool = np.nonzero(ok_mask)[0]
        if idx_pool.size == 0:
            idx_pool = np.arange(n_rows)

        # estratificar por mês (se existir)
        month_series = None
        if "DATA (YYYY-MM-DD)" in df_block.columns:
            # mais rápido que to_datetime em lotes grandes:
            # pega mm de 'YYYY-MM-DD'
            s = df_block["DATA (YYYY-MM-DD)"].astype("string")
            month_series = pd.to_numeric(s.str.slice(5, 7), errors="coerce").fillna(-1).astype(int)

        if month_series is None:
            # amostra simples
            if idx_pool.size <= max_rows:
                return idx_pool
            return rng.choice(idx_pool, size=max_rows, replace=False)

        # amostra estratificada por mês
        target = max_rows
        chosen: List[int] = []
        pool_df = pd.DataFrame({"idx": idx_pool, "m": month_series.iloc[idx_pool].to_numpy()})
        # distribuição proporcional por mês (m = 1..12; -1 = desconhecido)
        counts = pool_df["m"].value_counts().to_dict()
        total = float(len(pool_df))
        # passe 1: cota proporcional
        for m, cnt in counts.items():
            q = int(round(target * (cnt / total)))
            sub = pool_df[pool_df["m"] == m]["idx"].to_numpy()
            q = min(q, sub.size)
            if q > 0:
                chosen.extend(rng.choice(sub, size=q, replace=False).tolist())

        # complete se faltar
        chosen = np.array(chosen, dtype=int)
        if chosen.size < target:
            # pega do restante sem repetir
            rest = np.setdiff1d(idx_pool, chosen, assume_unique=False)
            need = min(target - chosen.size, rest.size)
            if need > 0:
                more = rng.choice(rest, size=need, replace=False)
                chosen = np.concatenate([chosen, more])
        elif chosen.size > target:
            chosen = rng.choice(chosen, size=target, replace=False)

        return np.sort(chosen)

    def _impute_block(
        self,
        df_block: pd.DataFrame,
        num_cols: List[str],
        n_neighbors: int,
    ) -> pd.DataFrame:
        """
        Imputa um bloco (tabela inteira ou subgrupo) usando KNNImputer,
        com FIT possivelmente em subamostra e TRANSFORM em blocos.
        """
        df_block = df_block.copy()
        X = df_block[num_cols].to_numpy(dtype=np.float32, copy=True)
        n_rows, n_cols = X.shape

        # estatísticas iniciais
        miss_counts = np.isnan(X).sum(axis=0)
        miss_pct = (miss_counts / max(1, n_rows)) * 100.0
        worst_idx = np.argsort(-miss_pct)[:10]
        worst_cols = [(num_cols[i], int(miss_counts[i]), float(miss_pct[i])) for i in worst_idx]

        log.info(
            f"[KNN] Bloco: {n_rows} linhas x {n_cols} col ({df_block.index.min()}..{df_block.index.max()}) | {_mem_info()}"
        )
        if worst_cols:
            top_str = "; ".join([f"{c}: {cnt} ({pct:.1f}%)" for c, cnt, pct in worst_cols])
            log.info(f"[KNN] TOP missing (antes): {top_str}")

        # ---- FIT em subamostra (se configurado) ----
        fit_idx = self._choose_fit_indices(
            X,
            df_block=df_block,
            max_rows=(self.imputer_fit_max_rows or 0),
            prefer_complete=self.imputer_prefer_complete_fit,
            max_missing_frac=self.imputer_fit_max_missing_frac,
        )
        fit_X = X if fit_idx.size == X.shape[0] else X[fit_idx]
        log.info(f"[KNN] Fit set: {fit_X.shape[0]} linhas (de {n_rows}). n_neighbors={n_neighbors}, weights={self.imputer_weights}")

        t0 = time.perf_counter()
        imputer = KNNImputer(n_neighbors=n_neighbors, weights=self.imputer_weights)
        imputer.fit(fit_X)
        t_fit = time.perf_counter() - t0
        log.info(f"[KNN] FIT concluído em {t_fit:.1f}s | {_mem_info()}")

        # ---- TRANSFORM em blocos (sempre no X inteiro) ----
        chunk = max(1, int(self.imputer_chunk_rows))
        out = np.empty_like(X, dtype=np.float32)
        processed = 0
        t_start = time.perf_counter()
        t_last_heartbeat = t_start

        log.info(f"[KNN] TRANSFORM em blocos de {chunk} linhas...")
        for start in range(0, n_rows, chunk):
            end = min(start + chunk, n_rows)
            block = X[start:end]
            # transforma contra _fit_X já armazenado no imputer
            out[start:end] = imputer.transform(block).astype(np.float32)
            processed = end

            now = time.perf_counter()
            if (now - t_last_heartbeat) >= self.log_heartbeat_sec:
                elapsed = now - t_start
                speed = processed / max(1e-9, elapsed)
                log.info(
                    f"[KNN] Progresso: {processed}/{n_rows} ({processed/n_rows:.1%}) | "
                    f"{speed:,.0f} linhas/s | {_mem_info()}"
                )
                t_last_heartbeat = now

        t_total = time.perf_counter() - t_start
        speed_final = n_rows / max(1e-9, t_total)
        log.info(f"[KNN] TRANSFORM terminou em {t_total:.1f}s | {speed_final:,.0f} lin/s | {_mem_info()}")

        # pós-estatística
        miss_after = np.isnan(out).sum(axis=0)
        miss_after_pct = (miss_after / max(1, n_rows)) * 100.0
        improved = []
        for i in range(n_cols):
            if abs(miss_after_pct[i] - miss_pct[i]) > 0.01:
                improved.append((num_cols[i], float(miss_pct[i]), float(miss_after_pct[i])))
        if improved:
            improved.sort(key=lambda x: (x[1] - x[2]), reverse=True)
            improved = improved[:10]
            ch_str = "; ".join([f"{c}: {bef:.1f}% -> {aft:.1f}%" for c, bef, aft in improved])
            log.info(f"[KNN] Redução de missing (top): {ch_str}")

        df_block[num_cols] = out
        return df_block

    def apply_knn_imputation(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        n_neighbors: int = 5,
    ) -> pd.DataFrame:
        """
        Imputa com KNN:
        - se imputer_group_by_month=True e existir DATA (YYYY-MM-DD), divide por mês quando
          o grupo tiver >= imputer_min_group_rows; senão cai para o bloco anual.
        - sempre usa FIT subamostrado (quando configurado) e TRANSFORM em blocos.
        """
        df = df.copy()
        num_cols = self.get_numeric_feature_columns(df, feature_cols)
        if not num_cols:
            log.warning("[KNN] Sem colunas numéricas de feature. Imputação ignorada.")
            return df

        if self.imputer_group_by_month and "DATA (YYYY-MM-DD)" in df.columns:
            # extrai o mês de forma rápida
            months = pd.to_numeric(
                df["DATA (YYYY-MM-DD)"].astype("string").str.slice(5, 7),
                errors="coerce"
            ).fillna(-1).astype(int)

            result = df.copy()
            big_group_idx = []
            small_idx = []

            for m in range(1, 13):
                idx = np.where(months.values == m)[0]
                if idx.size >= self.imputer_min_group_rows:
                    big_group_idx.append(idx)
                elif idx.size > 0:
                    small_idx.append(idx)

            # processa grupos grandes por mês
            for idx in big_group_idx:
                part = result.iloc[idx]
                part_imp = self._impute_block(part, num_cols, n_neighbors=n_neighbors)
                result.iloc[idx, result.columns.get_indexer(num_cols)] = part_imp[num_cols].to_numpy()

            # fallback: junta os grupos pequenos + mês desconhecido
            if small_idx:
                idx_small = np.concatenate(small_idx)
            else:
                idx_small = np.array([], dtype=int)
            idx_unk = np.where(months.values == -1)[0]
            rest = np.sort(np.concatenate([idx_small, idx_unk])) if idx_small.size or idx_unk.size else np.array([], dtype=int)

            if rest.size:
                part = result.iloc[rest]
                part_imp = self._impute_block(part, num_cols, n_neighbors=n_neighbors)
                result.iloc[rest, result.columns.get_indexer(num_cols)] = part_imp[num_cols].to_numpy()

            log.info("[KNN] Imputação (agrupada por mês) concluída.")
            return result

        # caso padrão: ano inteiro de uma vez
        return self._impute_block(df, num_cols, n_neighbors=n_neighbors)

    # ------------------------------
    # Drop rows com missing em features
    # ------------------------------
    def drop_rows_with_missing_features(self, df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
        df = df.copy()
        mask_missing = df[feature_cols].isna().any(axis=1)
        n_missing_rows = int(mask_missing.sum())
        n_total = int(df.shape[0])
        df_clean = df.loc[~mask_missing].reset_index(drop=True)
        log.info(f"[DROP] Removidas {n_missing_rows} linhas ({n_missing_rows/n_total:.4f}). Nova shape: {df_clean.shape}.")
        return df_clean

    # ------------------------------
    # Paths / save
    # ------------------------------
    def get_scenario_path(self, scenario: str, year: int) -> Path:
        return self.modeling_dir / scenario / f"inmet_bdq_{year}_cerrado.parquet"

    def save_scenario_year(self, df: pd.DataFrame, scenario: str, year: int) -> None:
        path = self.get_scenario_path(scenario, year)
        ensure_dir(path.parent)
        if path.exists() and not self.overwrite_existing:
            log.info(f"[SKIP] {scenario} ({year}) já existe em {path}.")
            return
        if path.exists() and self.overwrite_existing:
            log.info(f"[OVERWRITE] {scenario} ({year}) será sobrescrito.")
        df.to_parquet(path, index=False)
        log.info(f"[SAVE] {scenario} ({year}): {df.shape[0]} x {df.shape[1]} -> {path}")

    # ------------------------------
    # Pipeline de um ano
    # ------------------------------
    def build_and_save_scenarios_for_year(self, df_year: pd.DataFrame, year: int, n_neighbors: int = 5) -> None:
        if self.label_col not in df_year.columns:
            raise KeyError(f"Coluna de label '{self.label_col}' nao encontrada no ano {year}.")

        enabled_codes = [c for c in SCENARIO_ORDER if self.is_enabled(c)]

        if not self.overwrite_existing:
            if all(self.scenario_path_exists(c, year) for c in enabled_codes):
                log.info(f"[YEAR {year}] Todos os cenários habilitados {enabled_codes} já existem. Pulando.")
                return

        df_year = self.apply_missing_semantics(df_year)
        df_year = self.coerce_feature_columns_to_numeric(df_year)

        sort_cols = [c for c in ["ANO", "DATA (YYYY-MM-DD)", "HORA (UTC)"] if c in df_year.columns]
        if sort_cols:
            df_year = df_year.sort_values(sort_cols).reset_index(drop=True)
        log.info(f"[YEAR {year}] Base após missing/coerce: {df_year.shape[0]} x {df_year.shape[1]}.")

        df_A = None
        # F
        if "F" in enabled_codes:
            path_F = self.get_scenario_path(SCENARIO_MAP["F"], year)
            if path_F.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['F']} já existe.")
            else:
                self.save_scenario_year(df_year, SCENARIO_MAP["F"], year)

        need_no_rad = any(c in enabled_codes for c in ("A", "B", "C"))
        if need_no_rad:
            df_A = df_year.copy()
            if self.radiacao_col in df_A.columns:
                df_A = df_A.drop(columns=[self.radiacao_col])
                log.info(f"[YEAR {year}] Base sem radiação preparada (A/B/C).")
            else:
                log.warning(f"[YEAR {year}] Coluna '{self.radiacao_col}' não encontrada (A/B/C).")

        # A
        if "A" in enabled_codes:
            path_A = self.get_scenario_path(SCENARIO_MAP["A"], year)
            if path_A.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['A']} já existe.")
            else:
                self.save_scenario_year(df_A, SCENARIO_MAP["A"], year)

        # B
        if "B" in enabled_codes:
            path_B = self.get_scenario_path(SCENARIO_MAP["B"], year)
            if path_B.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['B']} já existe.")
            else:
                df_B = df_A.copy()
                feature_cols_no_rad = self.get_feature_columns(df_B)
                df_B = self.apply_knn_imputation(df_B, feature_cols_no_rad, n_neighbors=n_neighbors)
                self.save_scenario_year(df_B, SCENARIO_MAP["B"], year)

        # C
        if "C" in enabled_codes:
            path_C = self.get_scenario_path(SCENARIO_MAP["C"], year)
            if path_C.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['C']} já existe.")
            else:
                df_C = df_A.copy()
                feature_cols_no_rad = self.get_feature_columns(df_C)
                df_C = self.drop_rows_with_missing_features(df_C, feature_cols_no_rad)
                self.save_scenario_year(df_C, SCENARIO_MAP["C"], year)

        # D
        if "D" in enabled_codes:
            path_D = self.get_scenario_path(SCENARIO_MAP["D"], year)
            if path_D.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['D']} já existe.")
            else:
                df_D = df_year.copy()
                feature_cols_full = self.get_feature_columns(df_D)
                df_D = self.drop_rows_with_missing_features(df_D, feature_cols_full)
                self.save_scenario_year(df_D, SCENARIO_MAP["D"], year)

        # E
        if "E" in enabled_codes:
            path_E = self.get_scenario_path(SCENARIO_MAP["E"], year)
            if path_E.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['E']} já existe.")
            else:
                df_E = df_year.copy()
                feature_cols_full = self.get_feature_columns(df_E)
                df_E = self.apply_knn_imputation(df_E, feature_cols_full, n_neighbors=n_neighbors)
                self.save_scenario_year(df_E, SCENARIO_MAP["E"], year)

        log.info(f"[YEAR {year}] Concluído: cenários {enabled_codes}.")

    # ------------------------------
    # Vários anos
    # ------------------------------
    def run_for_years(self, years: Optional[List[int]] = None, n_neighbors: int = 5) -> None:
        year_files = self.discover_year_files()
        if years is not None:
            selected = {y: fp for y, fp in year_files.items() if y in years}
            if not selected:
                raise ValueError(
                    f"Nenhum dos anos {years} foi encontrado. Detectados: {sorted(year_files.keys())}"
                )
            year_files = dict(sorted(selected.items()))
        log.info(f"[RUN] Construindo {len(year_files)} ano(s).")
        for year, fp in year_files.items():
            log.info(f"[YEAR {year}] Lendo {fp.name}")
            df_year = self.read_year_csv(fp, year=year)
            self.build_and_save_scenarios_for_year(df_year=df_year, year=year, n_neighbors=n_neighbors)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Construcao das bases de modelagem (6 cenarios) a partir dos "
            "CSVs inmet_bdq_{ANO}_cerrado.\n"
            "Salva parquet em data/modeling/<cenario>/inmet_bdq_{ANO}_cerrado.parquet."
        )
    )

    parser.add_argument(
        "--pattern",
        default=FILENAME_PATTERN,
        help=f"Padrao dos arquivos (default: {FILENAME_PATTERN})",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help=(
            "Lista de anos a incluir na base. "
            "Se nao for informada, usa todos os anos detectados."
        ),
    )
    parser.add_argument(
        "--n-neighbors",
        type=int,
        default=5,
        help="Numero de vizinhos para o KNNImputer (default: 5).",
    )
    parser.add_argument(
        "--radiacao-col",
        type=str,
        default=RADIACAO_COL,
        help=f"Nome da coluna de radiacao global (default: {RADIACAO_COL!r}).",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help=(
            "Se informado, sobrescreve parquets ja existentes em data/modeling. "
            "Sem esta flag, arquivos existentes sao mantidos e o cenario/ano eh pulado."
        ),
    )
    parser.add_argument(
        "--only-scenarios",
        nargs="+",
        choices=list(SCENARIO_MAP.keys()),  # ["A","B","C","D","E","F"]
        help=(
            "Processa somente os cenarios indicados (letras A..F). "
            "Ex.: --only-scenarios A C F"
        ),
    )
    parser.add_argument(
        "--skip-scenarios",
        nargs="+",
        choices=list(SCENARIO_MAP.keys()),
        help=(
            "Pula os cenarios indicados (letras A..F). "
            "Ex.: --skip-scenarios E"
        ),
    )
    parser.add_argument(
        "--knn-chunk-rows",
        type=int,
        default=50_000,
        help="Qtde de linhas por bloco no transform do KNNImputer (default: 50k)."
    )
    parser.add_argument(
        "--log-heartbeat-sec",
        type=int,
        default=20,
        help="Intervalo (s) para heartbeat de log durante KNN (default: 20s)."
    )

    parser.add_argument("--knn-fit-max-rows", type=int, default=80_000,
        help="Máx. de linhas para FIT do KNNImputer; 0 usa todas (default: 80k).")
    parser.add_argument("--no-knn-prefer-complete-fit", dest="knn_prefer_complete_fit",
        action="store_false",
        help="Não prioriza linhas com menos NaN no conjunto de FIT (por padrão prioriza).")
    parser.set_defaults(knn_prefer_complete_fit=True)

    parser.add_argument("--knn-fit-max-missing-frac", type=float, default=0.40,
        help="Na priorização de linhas 'completas' no FIT, máximo de NaN por linha (fração). Default 0.40.")
    parser.add_argument("--knn-weights", choices=["uniform","distance"], default="uniform",
        help="Peso do KNNImputer (uniform|distance). Default: uniform.")

    parser.add_argument("--knn-group-by-month", action="store_true",
        help="Imputa por mês quando grupo >= --knn-min-group-rows; grupos menores caem no fallback anual.")
    parser.add_argument("--knn-min-group-rows", type=int, default=40_000,
        help="Tamanho mínimo para imputar por mês (default: 40k).")

    args = parser.parse_args()

    # Resolve conjunto final de cenarios habilitados
    if args.only_scenarios:
        enabled = set(args.only_scenarios)
    else:
        enabled = set(SCENARIO_ORDER)  # todos por padrao

    if args.skip_scenarios:
        enabled -= set(args.skip_scenarios)

    if not enabled:
        raise SystemExit("Nenhum cenario habilitado apos aplicar only/skip.")

    builder = ModelingDatasetBuilder(
        dataset_dir=DATASET_DIR,
        modeling_dir=MODELING_DIR,
        file_pattern=args.pattern,
        radiacao_col=args.radiacao_col,
        overwrite_existing=args.overwrite_existing,
        enabled_scenarios=enabled,
        imputer_chunk_rows=args.knn_chunk_rows,
        log_heartbeat_sec=args.log_heartbeat_sec,
        imputer_fit_max_rows=args.knn_fit_max_rows,
        imputer_prefer_complete_fit=args.knn_prefer_complete_fit,
        imputer_fit_max_missing_frac=args.knn_fit_max_missing_frac,
        imputer_weights=args.knn_weights,
        imputer_group_by_month=args.knn_group_by_month,
        imputer_min_group_rows=args.knn_min_group_rows,
    )

    log.info(
        f"[CONFIG] dataset_dir={builder.dataset_dir} "
        f"pattern={builder.file_pattern} "
        f"modeling_dir={builder.modeling_dir} "
        f"radiacao_col={builder.radiacao_col} "
        f"n_neighbors={args.n_neighbors} "
        f"overwrite_existing={builder.overwrite_existing} "
        f"enabled_scenarios={sorted(builder.enabled_scenarios)}"
    )

    builder.run_for_years(years=args.years, n_neighbors=args.n_neighbors)


if __name__ == "__main__":
    main()
