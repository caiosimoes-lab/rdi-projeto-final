"""
Integra State of Data Brazil (pesquisa) com vagas (RemoteOK)
e prepara um campo único de texto para Recuperação da Informação.
"""

from pathlib import Path
from typing import List

import json
import pandas as pd

STATE_FILE = Path("data/processed/state_of_data_clean.csv")
VAGAS_FILE = Path("data/raw/external/remoteok_jobs.json")
OUT_FILE = Path("data/processed/base_integrada.csv")


def carregar_state_of_data(path: Path = STATE_FILE) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[Integracao] Base limpa da pesquisa não encontrada: {path}")
    df = pd.read_csv(path)
    df["fonte"] = "state_of_data"
    return df


def carregar_vagas_remoteok(path: Path = VAGAS_FILE) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"[Integracao] Arquivo de vagas não encontrado: {path}")

    payload = json.loads(path.read_text(encoding="utf-8"))
    jobs = payload.get("jobs", [])
    df = pd.DataFrame(jobs)

    if df.empty:
        print("[Integracao] Nenhuma vaga carregada da API.")
        return df

    colunas_interesse = [
        col
        for col in ["id", "date", "company", "position", "tags", "location", "description"]
        if col in df.columns
    ]
    df = df[colunas_interesse]
    df["fonte"] = "remoteok"

    return df


def criar_texto_ir_pesquisa(df: pd.DataFrame) -> pd.Series:
    # aqui podemos escolher campos mais interessantes depois;
    # por enquanto, concatenamos tudo.
    return df.astype(str).agg(" | ".join, axis=1)


def criar_texto_ir_vagas(df: pd.DataFrame) -> pd.Series:
    campos = [c for c in ["position", "company", "location", "tags", "description"] if c in df.columns]
    if not campos:
        return df.astype(str).agg(" | ".join, axis=1)
    return df[campos].astype(str).agg(" | ".join, axis=1)


def integrar_bases(
    state_file: Path = STATE_FILE,
    vagas_file: Path = VAGAS_FILE,
    out_file: Path = OUT_FILE,
) -> Path:
    df_state = carregar_state_of_data(state_file)
    df_vagas = carregar_vagas_remoteok(vagas_file)

    if df_state.empty and df_vagas.empty:
        raise RuntimeError("[Integracao] Nenhuma base disponível para integrar.")

    df_state = df_state.copy()
    df_state["tipo_documento"] = "resposta_pesquisa"
    df_state["texto_ir"] = criar_texto_ir_pesquisa(df_state)

    if not df_vagas.empty:
        df_vagas = df_vagas.copy()
        df_vagas["tipo_documento"] = "vaga_trabalho"
        df_vagas["texto_ir"] = criar_texto_ir_vagas(df_vagas)

        df_all = pd.concat([df_state, df_vagas], ignore_index=True)
    else:
        df_all = df_state

    out_file.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(out_file, index=False)

    print(f"[Integracao] Base integrada salva em: {out_file.resolve()}")
    print(f"[Integracao] Linhas: {len(df_all)}")
    return out_file


if __name__ == "__main__":
    integrar_bases()
