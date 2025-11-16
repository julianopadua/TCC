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


# -----------------------------------------------------------------------------
# CLASSE PRINCIPAL
# -----------------------------------------------------------------------------
@dataclass
class ModelingDatasetBuilder:
    dataset_dir: Path
    modeling_dir: Path
    file_pattern: str = FILENAME_PATTERN
    missing_codes: set[int] = field(default_factory=lambda: MISSING_CODES.copy())
    exclude_non_numeric: set[str] = field(
        default_factory=lambda: EXCLUDE_NON_NUMERIC.copy()
    )
    target_cols: set[str] = field(default_factory=lambda: TARGET_COLS.copy())
    label_col: str = LABEL_COL
    radiacao_col: str = RADIACAO_COL
    # Por default, NAO sobrescreve parquets ja existentes.
    overwrite_existing: bool = False
    enabled_scenarios: set[str] = field(default_factory=lambda: set(SCENARIO_ORDER))


    def __post_init__(self) -> None:
        self.missing_codes_str = {str(v) for v in self.missing_codes}

    # ------------------------------
    # Descoberta de arquivos
    # ------------------------------
    def discover_year_files(self) -> Dict[int, Path]:
        """
        Localiza arquivos ano a ano pelo padrao informado e extrai o ano do nome.
        Retorna {ano: caminho}.
        """
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
        """Retorna True se o cenário (A..F) está habilitado."""
        return code in self.enabled_scenarios

    def scenario_path_exists(self, code: str, year: int) -> bool:
        """Checa existência do parquet do cenário code (A..F) para o ano."""
        return self.get_scenario_path(SCENARIO_MAP[code], year).exists()

    # ------------------------------
    # Harmonizacao de colunas
    # ------------------------------
    def harmonize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza inconsistencias de nomenclatura entre anos.
        Exemplo principal: unificar radiacao global Kj/m² e KJ/m².
        """
        df = df.copy()

        old = "RADIACAO GLOBAL (Kj/m²)"
        new = self.radiacao_col

        if old in df.columns and new in df.columns:
            filled = df[new].combine_first(df[old])
            conflict_mask = (
                df[old].notna()
                & df[new].notna()
                & (df[old] != df[new])
            )
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

    # ------------------------------
    # Leitura de CSV anual
    # ------------------------------
    def read_year_csv(self, fp: Path, year: int) -> pd.DataFrame:
        df = pd.read_csv(
            fp,
            sep=",",
            decimal=",",  # trata "898,1" como 898.1, etc.
            encoding="utf-8",
            low_memory=False,
        )
        df = self.harmonize_columns(df)

        # Coluna ANO, se nao existir
        if "ANO" not in df.columns:
            df["ANO"] = year

        return df

    # ------------------------------
    # Aplicar semantica de missing (NaN, vazios, sentinelas) ANO A ANO
    # ------------------------------
    def apply_missing_semantics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte:
        - -999 / -9999 em NaN nas colunas numericas.
        - strings vazias em NaN nas colunas texto.
        """
        df = df.copy()

        for c in df.columns:
            s = df[c]

            if pd.api.types.is_numeric_dtype(s):
                # substitui sentinelas por NaN
                df[c] = s.replace(list(self.missing_codes), pd.NA)
            else:
                # tratamento leve para texto: vazio -> NaN
                s_str = s.astype("string")
                df[c] = s_str.mask(s_str.str.strip().eq(""))

        return df

    # ------------------------------
    # Forcar features a numerico (pos missing_semantics)
    # ------------------------------
    def coerce_feature_columns_to_numeric(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Forca as colunas de feature a serem numericas via pd.to_numeric(errors='coerce').
        Isso garante que o KNNImputer enxergue todas como numericas,
        desde que os valores sejam de fato numericos ou NaN.
        """
        df = df.copy()
        feature_cols = self.get_feature_columns(df)

        for c in feature_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        n_num = sum(pd.api.types.is_numeric_dtype(df[c]) for c in feature_cols)
        log.info(
            f"[COERCE] {len(feature_cols)} features candidatas; "
            f"{n_num} colunas numericas apos to_numeric."
        )
        return df

    # ------------------------------
    # Determinar colunas de feature
    # ------------------------------
    def get_feature_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Features = todas as colunas exceto:
        - TARGET_COLS
        - LABEL_COL
        - EXCLUDE_NON_NUMERIC
        """
        feature_cols: List[str] = []
        for c in df.columns:
            if c in self.target_cols:
                continue
            if c == self.label_col:
                continue
            if c in self.exclude_non_numeric:
                continue
            feature_cols.append(c)
        log.info(f"[FEATURES] {len(feature_cols)} colunas de feature encontradas: {feature_cols}")
        return feature_cols

    def get_numeric_feature_columns(self, df: pd.DataFrame, feature_cols: List[str]) -> List[str]:
        num_cols = [c for c in feature_cols if pd.api.types.is_numeric_dtype(df[c])]
        log.info(f"[NUM FEATURES] {len(num_cols)} colunas numericas para KNNImputer: {num_cols}")
        return num_cols

    # ------------------------------
    # Imputacao KNN em features numericas
    # ------------------------------
    def apply_knn_imputation(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
        n_neighbors: int = 5,
    ) -> pd.DataFrame:
        """
        Aplica KNNImputer somente nas colunas numericas de feature.
        (Feito por ano, para evitar estouro de memoria.)
        """
        df = df.copy()

        num_cols = self.get_numeric_feature_columns(df, feature_cols)
        if not num_cols:
            log.warning("[KNN] Nenhuma coluna numerica de feature encontrada. Imputacao ignorada.")
            return df

        imputer = KNNImputer(n_neighbors=n_neighbors, weights="uniform")

        log.info(f"[KNN] Ajustando imputador com n_neighbors={n_neighbors}.")
        num_values = df[num_cols].to_numpy()
        imputed = imputer.fit_transform(num_values)

        df[num_cols] = imputed
        log.info("[KNN] Imputacao concluida para este ano.")
        return df

    # ------------------------------
    # Remover linhas que tenham qualquer feature faltante
    # ------------------------------
    def drop_rows_with_missing_features(
        self,
        df: pd.DataFrame,
        feature_cols: List[str],
    ) -> pd.DataFrame:
        """
        Remove linhas que tenham NaN em qualquer coluna de feature.
        (Feito por ano.)
        """
        df = df.copy()
        mask_missing = df[feature_cols].isna().any(axis=1)
        n_missing_rows = int(mask_missing.sum())
        n_total = int(df.shape[0])

        df_clean = df.loc[~mask_missing].reset_index(drop=True)
        log.info(
            f"[DROP] Removidas {n_missing_rows} linhas com missing em features "
            f"({n_missing_rows/n_total:.4f} da base). Nova shape: {df_clean.shape}."
        )
        return df_clean

    # ------------------------------
    # Helper: caminho do parquet de um cenario/ano
    # ------------------------------
    def get_scenario_path(self, scenario: str, year: int) -> Path:
        """
        Retorna o caminho completo do parquet para um dado cenario e ano,
        sem criar o diretorio ainda.
        """
        return self.modeling_dir / scenario / f"inmet_bdq_{year}_cerrado.parquet"


    # ------------------------------
    # Helper para salvar cenarios (por ano)
    # ------------------------------
    def save_scenario_year(self, df: pd.DataFrame, scenario: str, year: int) -> None:
        """
        Salva um parquet para um determinado cenario e ano.
        Ex: data/modeling/base_A_no_rad/inmet_bdq_2004_cerrado.parquet

        Por default NAO sobrescreve se o arquivo ja existir. Para forcar
        sobrescrita, use overwrite_existing=True no builder ou --overwrite-existing na CLI.
        """
        path = self.get_scenario_path(scenario, year)
        scenario_dir = ensure_dir(path.parent)

        if path.exists() and not self.overwrite_existing:
            log.info(
                f"[SKIP] {scenario} ({year}) ja existe em {path}, nao sobrescrevendo "
                f"(overwrite_existing={self.overwrite_existing})."
            )
            return

        if path.exists() and self.overwrite_existing:
            log.info(
                f"[OVERWRITE] {scenario} ({year}) ja existia em {path}, sera sobrescrito."
            )

        df.to_parquet(path, index=False)
        log.info(
            f"[SAVE] {scenario} ({year}): {df.shape[0]} linhas, {df.shape[1]} colunas -> {path}"
        )


    def build_and_save_scenarios_for_year(
        self,
        df_year: pd.DataFrame,
        year: int,
        n_neighbors: int = 5,
    ) -> None:
        """
        Constroi e salva as bases (apenas as habilitadas em self.enabled_scenarios) para um ano.

        Regras:
        - Se TODOS os cenários habilitados já existem e overwrite_existing=False, pula o ano.
        - Para cada cenário, só computa o necessário (evita montar df_A se A/B/C não estiverem habilitados).
        - Evita rodar KNN/descarte se o parquet do cenário já existir (quando overwrite_existing=False).
        """
        if self.label_col not in df_year.columns:
            raise KeyError(
                f"Coluna de label '{self.label_col}' nao encontrada no ano {year}."
            )

        # Lista de códigos de cenário realmente habilitados (na ordem canônica)
        enabled_codes = [c for c in SCENARIO_ORDER if self.is_enabled(c)]

        # Se todos os parquets dos cenários habilitados existem, pula tudo
        if not self.overwrite_existing:
            if all(self.scenario_path_exists(c, year) for c in enabled_codes):
                log.info(f"[YEAR {year}] Todos os cenarios habilitados {enabled_codes} ja existem. Pulando ano.")
                return

        # Só agora aplicamos missing/coerce se houver algo para fazer
        df_year = self.apply_missing_semantics(df_year)
        df_year = self.coerce_feature_columns_to_numeric(df_year)

        sort_cols = [c for c in ["ANO", "DATA (YYYY-MM-DD)", "HORA (UTC)"] if c in df_year.columns]
        if sort_cols:
            df_year = df_year.sort_values(sort_cols).reset_index(drop=True)

        log.info(f"[YEAR {year}] Base original apos missing/coerce: {df_year.shape[0]} linhas, {df_year.shape[1]} colunas.")

        # Cache leve para reaproveitar dataframes intermediários somente quando necessário
        df_A = None  # versão "sem radiacao" se precisarmos de A/B/C

        # --------------------------
        # F) full original
        # --------------------------
        if "F" in enabled_codes:
            path_F = self.get_scenario_path(SCENARIO_MAP["F"], year)
            if path_F.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['F']} ja existe.")
            else:
                self.save_scenario_year(df_year, SCENARIO_MAP["F"], year)

        # Precisaremos da base sem radiação se A ou B ou C estiverem habilitados
        need_no_rad = any(c in enabled_codes for c in ("A", "B", "C"))
        if need_no_rad:
            df_A = df_year.copy()
            if self.radiacao_col in df_A.columns:
                df_A = df_A.drop(columns=[self.radiacao_col])
                log.info(f"[YEAR {year}] Base sem radiacao preparada para A/B/C.")
            else:
                log.warning(f"[YEAR {year}] Coluna de radiacao '{self.radiacao_col}' nao encontrada (A/B/C).")

        # --------------------------
        # A) sem radiacao
        # --------------------------
        if "A" in enabled_codes:
            path_A = self.get_scenario_path(SCENARIO_MAP["A"], year)
            if path_A.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['A']} ja existe.")
            else:
                self.save_scenario_year(df_A, SCENARIO_MAP["A"], year)

        # --------------------------
        # B) sem radiacao + KNN
        # --------------------------
        if "B" in enabled_codes:
            path_B = self.get_scenario_path(SCENARIO_MAP["B"], year)
            if path_B.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['B']} ja existe.")
            else:
                df_B = df_A.copy()
                feature_cols_no_rad = self.get_feature_columns(df_B)
                df_B = self.apply_knn_imputation(df_B, feature_cols_no_rad, n_neighbors=n_neighbors)
                self.save_scenario_year(df_B, SCENARIO_MAP["B"], year)

        # --------------------------
        # C) sem radiacao + drop rows
        # --------------------------
        if "C" in enabled_codes:
            path_C = self.get_scenario_path(SCENARIO_MAP["C"], year)
            if path_C.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['C']} ja existe.")
            else:
                df_C = df_A.copy()
                feature_cols_no_rad = self.get_feature_columns(df_C)
                df_C = self.drop_rows_with_missing_features(df_C, feature_cols_no_rad)
                self.save_scenario_year(df_C, SCENARIO_MAP["C"], year)

        # --------------------------
        # D) com radiacao + drop rows
        # --------------------------
        if "D" in enabled_codes:
            path_D = self.get_scenario_path(SCENARIO_MAP["D"], year)
            if path_D.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['D']} ja existe.")
            else:
                df_D = df_year.copy()
                feature_cols_full = self.get_feature_columns(df_D)
                df_D = self.drop_rows_with_missing_features(df_D, feature_cols_full)
                self.save_scenario_year(df_D, SCENARIO_MAP["D"], year)

        # --------------------------
        # E) com radiacao + KNN
        # --------------------------
        if "E" in enabled_codes:
            path_E = self.get_scenario_path(SCENARIO_MAP["E"], year)
            if path_E.exists() and not self.overwrite_existing:
                log.info(f"[YEAR {year}][SKIP] {SCENARIO_MAP['E']} ja existe.")
            else:
                df_E = df_year.copy()
                feature_cols_full = self.get_feature_columns(df_E)
                df_E = self.apply_knn_imputation(df_E, feature_cols_full, n_neighbors=n_neighbors)
                self.save_scenario_year(df_E, SCENARIO_MAP["E"], year)

        log.info(f"[YEAR {year}] Processamento concluido para cenarios {enabled_codes}.")

    # ------------------------------
    # Pipeline principal: processar varios anos
    # ------------------------------
    def run_for_years(
        self,
        years: List[int] | None = None,
        n_neighbors: int = 5,
    ) -> None:
        """
        Processa anos (todos ou subset) gerando as 6 bases ano a ano.
        """
        year_files = self.discover_year_files()

        if years is not None:
            selected = {y: fp for y, fp in year_files.items() if y in years}
            if not selected:
                raise ValueError(
                    f"Nenhum dos anos especificados {years} foi encontrado entre "
                    f"os arquivos detectados: {sorted(year_files.keys())}"
                )
            year_files = dict(sorted(selected.items()))

        log.info(
            f"[RUN] Construindo cenarios de modelagem para {len(year_files)} anos."
        )

        for year, fp in year_files.items():
            log.info(f"[YEAR {year}] Lendo arquivo {fp.name}")
            df_year = self.read_year_csv(fp, year=year)
            self.build_and_save_scenarios_for_year(
                df_year=df_year,
                year=year,
                n_neighbors=n_neighbors,
            )


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

    # ===== NOVO: seleção de cenários =====
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
