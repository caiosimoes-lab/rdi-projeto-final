import os
import json
from pathlib import Path

import requests


def coletar_remoteok(dest_file: str = "data/raw/external/remoteok_jobs.json") -> None:
    """Coleta vagas remotas de tecnologia usando a API pública do RemoteOK.

    Esta função é um exemplo de fonte externa para o trabalho:
    você pode filtrar depois apenas vagas relacionadas a Data / Analytics.
    """
    url = "https://remoteok.com/api"
    print(f"Requisitando {url} ...")
    resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()

    data = resp.json()

    dest_path = Path(dest_file)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with dest_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print("Vagas salvas em:", dest_path.resolve())


if __name__ == "__main__":
    coletar_remoteok()
