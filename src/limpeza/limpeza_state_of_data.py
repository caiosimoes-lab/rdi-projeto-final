import os
from pathlib import Path
import pandas as pd


def carregar_arquivos_state_of_data(raw_dir: str = "data/raw/state_of_data"):
    raw_path = Path(raw_dir)
    arquivos = list(raw_path.glob("*.csv"))
    if not arquivos:
        raise FileNotFoundError(
            f"Nenhum CSV encontrado em {raw_path}. "
            "Certifique-se de que a coleta foi executada."
        )
    dfs = []
    for arq in arquivos:
        print("Lendo:", arq)
        df = pd.read_csv(arq)
        df["arquivo_origem"] = arq.name
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def limpar_state_of_data(df: pd.DataFrame) -> pd.DataFrame:
    # Exemplo de limpeza mínima. Ajuste conforme os nomes reais das colunas.
    df = df.copy()
    # Padronizar nomes das colunas
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Remover duplicados
    df = df.drop_duplicates()

    # Exemplo: remover linhas totalmente vazias
    df = df.dropna(how="all")

    return df


def pipeline_limpeza_state_of_data(
    raw_dir: str = "data/raw/state_of_data",
    out_file: str = "data/processed/state_of_data_clean.csv",
) -> None:
    df = carregar_arquivos_state_of_data(raw_dir)
    df_clean = limpar_state_of_data(df)

    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(out_path, index=False)
    print("Arquivo limpo salvo em:", out_path.resolve())


if __name__ == "__main__":
    pipeline_limpeza_state_of_data()
