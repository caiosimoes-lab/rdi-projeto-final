"""
Coleta State of Data Brazil (2021–2025) via Kaggle.

Requisitos:
- Conta no Kaggle
- kagglehub instalado (`pip install kagglehub`)
- Token do Kaggle configurado (~/.kaggle/kaggle.json)
"""

from pathlib import Path
from typing import Optional

try:
    import kagglehub
except ImportError as exc:
    raise ImportError(
        "O pacote 'kagglehub' não está instalado. "
        "Instale com `pip install kagglehub`."
    ) from exc


DATASET_SLUG = "basedosdados/state-of-data-brazil"
RAW_DIR = Path("data/raw/state_of_data")


def baixar_state_of_data(dest_dir: Optional[Path] = None, force: bool = False) -> Path:
    """
    Baixa os arquivos do dataset State of Data Brazil e copia para dest_dir.

    Parameters
    ----------
    dest_dir : Path, optional
        Diretório de destino onde os arquivos serão salvos.
        Default: data/raw/state_of_data
    force : bool
        Se True, baixa novamente mesmo se já existirem arquivos.

    Returns
    -------
    Path
        Caminho final onde os arquivos foram salvos.
    """
    dest_dir = dest_dir or RAW_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Se já existem arquivos e não força, não faz nada
    if not force and any(dest_dir.glob("*.csv")):
        print(f"[StateOfData] Arquivos já encontrados em {dest_dir}, pulando download.")
        return dest_dir

    print(f"[StateOfData] Baixando dataset '{DATASET_SLUG}' via kagglehub...")
    path = Path(kagglehub.dataset_download(DATASET_SLUG))
    print(f"[StateOfData] Arquivos baixados para cache em: {path}")

    # Copia todos os CSVs para o diretório raw do projeto
    num_copiados = 0
    for file in path.rglob("*.csv"):
        target = dest_dir / file.name
        target.write_bytes(file.read_bytes())
        num_copiados += 1
        print(f"[StateOfData] Copiado: {file.name}")

    if num_copiados == 0:
        print("[StateOfData] Nenhum CSV encontrado no dataset baixado.")

    print(f"[StateOfData] Concluído. Arquivos em: {dest_dir.resolve()}")
    return dest_dir


if __name__ == "__main__":
    baixar_state_of_data()

