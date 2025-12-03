from pathlib import Path
import json
import pandas as pd


def carregar_vagas_remoteok(raw_file: str = "data/raw/external/remoteok_jobs.json") -> pd.DataFrame:
    path = Path(raw_file)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de vagas não encontrado: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))

    # A API retorna uma lista de dicionários; ignorar o primeiro item se for metadata
    if data and isinstance(data[0], dict) and "id" not in data[0]:
        data = data[1:]

    df = pd.DataFrame(data)
    # selecionar apenas algumas colunas relevantes
    colunas = [c for c in ["id", "date", "company", "position", "tags", "location", "description"] if c in df.columns]
    df = df[colunas]
    return df


def integrar_bases(
    state_file: str = "data/processed/state_of_data_clean.csv",
    vagas_file: str = "data/raw/external/remoteok_jobs.json",
    out_file: str = "data/processed/base_integrada.csv",
) -> None:
    df_state = pd.read_csv(state_file)
    df_vagas = carregar_vagas_remoteok(vagas_file)

    # Exemplo simples: apenas salvar as duas tabelas em um "dataset integrado" com informação de origem
    df_state["fonte"] = "state_of_data"
    df_vagas["fonte"] = "vagas_remoteok"

    df_state["texto_ir"] = (
        df_state.astype(str).agg(" | ".join, axis=1)
    )

    df_vagas["texto_ir"] = (
        df_vagas.astype(str).agg(" | ".join, axis=1)
    )

    df_all = pd.concat([df_state, df_vagas], ignore_index=True)

    out_path = Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out_path, index=False)
    print("Base integrada salva em:", out_path.resolve())


if __name__ == "__main__":
    integrar_bases()
