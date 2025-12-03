"""
Coleta de vagas remotas de tecnologia via API pública do RemoteOK.

A ideia é usar essa base como "proxy" do mercado de trabalho em Dados:
depois filtramos no notebook / scripts por palavras-chave de interesse.
"""

from pathlib import Path
from typing import List

import json
import datetime as dt

import requests

RAW_FILE = Path("data/raw/external/remoteok_jobs.json")


def _filtrar_vagas_data(jobs: List[dict]) -> List[dict]:
    """
    Mantém apenas vagas que parecem relacionadas à área de Dados/Analytics.

    Critério bem simples (pode ser refinado depois):
    - título ou tags contendo palavras como: data, analytics, scientist, engineer
    """
    palavras = ("data", "analytics", "analyst", "scientist", "engineer", "bi")
    filtradas = []

    for job in jobs:
        title = str(job.get("position") or job.get("title") or "").lower()
        tags = " ".join(job.get("tags") or []).lower()

        texto = f"{title} {tags}"
        if any(p in texto for p in palavras):
            filtradas.append(job)

    return filtradas


def coletar_remoteok(dest_file: Path = RAW_FILE, apenas_dados: bool = True) -> Path:
    """
    Coleta vagas da API RemoteOK e salva em JSON.

    Parameters
    ----------
    dest_file : Path
        Caminho do arquivo JSON de saída.
    apenas_dados : bool
        Se True, mantém apenas vagas relacionadas a Dados/Analytics.

    Returns
    -------
    Path
        Caminho final do arquivo salvo.
    """
    url = "https://remoteok.com/api"
    print(f"[RemoteOK] Requisitando {url} ...")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    data = resp.json()
    # Primeiro item às vezes é metadado, não vaga
    if data and not isinstance(data[0].get("id", None), (int, str)):
        data = data[1:]

    print(f"[RemoteOK] Vagas retornadas: {len(data)}")

    if apenas_dados:
        data = _filtrar_vagas_data(data)
        print(f"[RemoteOK] Vagas relacionadas a Dados/Analytics: {len(data)}")

    dest_file.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "metadata": {
            "fonte": "remoteok",
            "coletado_em": dt.datetime.utcnow().isoformat() + "Z",
            "apenas_dados": apenas_dados,
        },
        "jobs": data,
    }

    dest_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[RemoteOK] Arquivo salvo em: {dest_file.resolve()}")
    return dest_file


if __name__ == "__main__":
    coletar_remoteok()
