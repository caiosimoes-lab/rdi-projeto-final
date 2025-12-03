"""
Limpeza e padronização da pesquisa State of Data Brazil.

Como os CSVs podem variar um pouco entre anos, fazemos uma limpeza genérica:
- padroniza nomes de colunas
- adiciona coluna 'ano' quando possível
- remove duplicados e linhas totalmente vazias
"""

from pathlib import Path
from typing import List

import pandas as pd

RAW_DIR = Path("data/raw/state_of_data")
OUT_FILE = Path("data/processed/state_of_data_clean.csv")


def _inferir_ano(nome_arquivo: str) -> str:
    """Tenta extrair o ano a partir do nome do arquivo, fallback 'desconhecido'."""
    for ano in ("2021", "2022", "2023", "2024", "2025"):
        if ano in nome_arquivo:
            return ano
    return "desconhecido"


def carregar_arquivos_state_of_data(raw_dir: Path = RAW_DIR) -> List[pd.DataFrame]:
    """
    Lê todos os CSVs de State of Data e devolve uma lista de DataFrames.
    """
    if not raw_dir.exists():
        raise FileNotFoundError(
            f"Diretório {raw_dir} não encontrado. Rode a coleta antes."
        )

    arquivos = list(raw_dir.glob("*.csv"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum CSV encontrado em {raw_dir}.")

    dfs = []
    for arq in arquivos:
        print(f"[StateClean] Lendo {arq.name} ...")
        df = pd.read_csv(arq)
        df["arquivo_origem"] = arq.name
        df["ano"] = _inferir_ano(arq.name)
        dfs.append(df)

    return dfs


def limpar_state_of_data(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Aplica limpeza genérica:
    - padroniza nomes de colunas
    - remove linhas totalmente vazias
    - remove duplicados
    """
    df = pd.concat(dfs, ignore_index=True)

    # padronizar nomes de colunas
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^0-9a-zA-Z_]", "", regex=True)
    )

    # remover linhas completamente vazias
    df = df.dropna(how="all")

    # remover duplicados
    df = df.drop_duplicates()

    print(f"[StateClean] Após limpeza: {df.shape[0]} linhas, {df.shape[1]} colunas.")
    return df


def pipeline_limpeza_state_of_data(
    raw_dir: Path = RAW_DIR,
    out_file: Path = OUT_FILE,
) -> Path:
    dfs = carregar_arquivos_state_of_data(raw_dir)
    df_clean = limpar_state_of_data(dfs)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(out_file, index=False)
    print(f"[StateClean] Arquivo limpo salvo em: {out_file.resolve()}")
    return out_file


if __name__ == "__main__":
    pipeline_limpeza_state_of_data()
