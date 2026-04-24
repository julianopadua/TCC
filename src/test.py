from pathlib import Path

import pandas as pd

# Root do projeto inferido automaticamente pelo local do script.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ROOT_COORDS = PROJECT_ROOT / "data" / "_article" / "0_datasets_with_coords"


def find_col(columns: list[str], match: str) -> str | None:
    cols_upper = {c.upper(): c for c in columns}
    found = [cols_upper[k] for k in cols_upper if match in k]
    return found[0] if found else None


def auditar_arquivo(arquivo: Path, base_name: str) -> dict | None:
    try:
        df = pd.read_parquet(arquivo)
    except Exception as exc:
        return {
            "Base": base_name,
            "Arquivo": arquivo.name,
            "Erro": str(exc),
        }

    col_cidade = find_col(df.columns.tolist(), "CIDADE")
    col_hora = find_col(df.columns.tolist(), "HORA")
    col_data = find_col(df.columns.tolist(), "DATA")
    col_has = find_col(df.columns.tolist(), "HAS_FOCO")
    col_foco_id = find_col(df.columns.tolist(), "FOCO_ID")

    if not all([col_cidade, col_hora, col_data, col_has]):
        return {
            "Base": base_name,
            "Arquivo": arquivo.name,
            "Erro": "Colunas essenciais ausentes (CIDADE/HORA/DATA/HAS_FOCO).",
        }

    chave_basica = [col_cidade, col_data, col_hora]
    total_linhas = len(df)
    dups_exatas = int(df.duplicated().sum())

    df_0 = df[df[col_has] == 0]
    df_1 = df[df[col_has] == 1]

    dups_0 = int(df_0.duplicated(subset=chave_basica).sum()) if not df_0.empty else 0
    if col_foco_id:
        chave_com_foco = chave_basica + [col_foco_id]
        dups_1 = int(df_1.duplicated(subset=chave_com_foco).sum()) if not df_1.empty else 0
    else:
        dups_1 = int(df_1.duplicated(subset=chave_basica).sum()) if not df_1.empty else 0

    return {
        "Base": base_name,
        "Arquivo": arquivo.name,
        "Total": total_linhas,
        "Dups Exatas": dups_exatas,
        "Dups S/ Foco (Chave)": dups_0,
        "Dups C/ Foco (ID)": dups_1,
        "Erro": "",
    }


def auditoria_coords_todas_bases() -> None:
    print("\n" + "=" * 70)
    print("AUDITORIA DE DUPLICATAS — 0_DATASETS_WITH_COORDS (TODAS AS BASES)")
    print("=" * 70)
    print(f"Root analisado: {ROOT_COORDS}")

    if not ROOT_COORDS.exists():
        print(f"\nERRO: diretório não encontrado: {ROOT_COORDS}")
        return

    bases = sorted([p for p in ROOT_COORDS.iterdir() if p.is_dir()])
    if not bases:
        print("\nERRO: nenhuma base encontrada em 0_datasets_with_coords.")
        return

    all_results: list[dict] = []
    out_dir = PROJECT_ROOT / "data" / "_article" / "logs"
    out_dir.mkdir(parents=True, exist_ok=True)

    for base_dir in bases:
        base_name = base_dir.name
        arquivos = sorted(base_dir.glob("*.parquet"))
        print(f"\nBase: {base_name} | arquivos: {len(arquivos)}")

        if not arquivos:
            all_results.append(
                {"Base": base_name, "Arquivo": "", "Erro": "Nenhum .parquet encontrado."}
            )
            continue

        base_results: list[dict] = []
        for arquivo in arquivos:
            print(f"  Processando {arquivo.name}...", end="\r")
            result = auditar_arquivo(arquivo, base_name)
            if result:
                base_results.append(result)
        print(" " * 80, end="\r")

        df_base = pd.DataFrame(base_results)
        csv_base = out_dir / f"audit_coords_{base_name}.csv"
        df_base.to_csv(csv_base, index=False, sep=";")
        print(f"  OK -> {csv_base}")

        all_results.extend(base_results)

    df_all = pd.DataFrame(all_results)
    csv_all = out_dir / "audit_coords_all_bases.csv"
    df_all.to_csv(csv_all, index=False, sep=";")

    print("\n" + "=" * 70)
    print(f"Consolidado salvo em: {csv_all}")
    if not df_all.empty:
        resumo = (
            df_all[df_all.get("Erro", "") == ""]
            .groupby("Base", as_index=False)[["Dups Exatas", "Dups S/ Foco (Chave)", "Dups C/ Foco (ID)"]]
            .sum()
        )
        print("\nResumo de duplicatas por base:")
        print(resumo.to_string(index=False))
    print("=" * 70 + "\n")


if __name__ == "__main__":
    auditoria_coords_todas_bases()