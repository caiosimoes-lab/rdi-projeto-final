"""
Coleta State of Data Brazil (2021–2025) via Kaggle.

Usa os datasets oficiais da Data Hackers no Kaggle:

- datahackers/state-of-data-2021        (2021–2022)
- datahackers/state-of-data-2022        (2022–2023)
- datahackers/state-of-data-brazil-2023 (2023–2024)
- datahackers/state-of-data-brazil-20242025 (2024–2025)

É necessário ter conta no Kaggle + token configurado (~/.kaggle/kaggle.json).
"""

from pathlib import Path
from typing import Optional, List

try:
    import kagglehub
except ImportError as exc:
    raise ImportError(
        "O pacote 'kagglehub' não está instalado. "
        "Instale com `pip install kagglehub`."
    ) from exc


# Lista de datasets do Kaggle (Data Hackers)
DATASETS = [
    ("datahackers/state-of-data-2021", "state_of_data_2021"),
    ("datahackers/state-of-data-2022", "state_of_data_2022"),
    ("datahackers/state-of-data-brazil-2023", "state_of_data_2023"),
    ("datahackers/state-of-data-brazil-20242025", "state_of_data_2024_2025"),
]

RAW_DIR = Path("data/raw/state_of_data")


def baixar_dataset(slug: str, prefixo_nome: str, dest_dir: Path) -> None:
    """Baixa um único dataset do Kaggle e copia os CSVs para dest_dir."""
    print(f"[StateOfData] Baixando dataset '{slug}' via kagglehub...")
    path = Path(kagglehub.dataset_download(slug))
    print(f"[StateOfData] Cache: {path}")

    num = 0
    for file in path.rglob("*.csv"):
        # renomeia para incluir o prefixo, pra não sobrescrever
        target = dest_dir / f"{prefixo_nome}__{file.name}"
        target.write_bytes(file.read_bytes())
        num += 1
        print(f"[StateOfData] Copiado: {file.name} -> {target.name}")

    if num == 0:
        print(f"[StateOfData] Aviso: nenhum CSV encontrado em '{slug}'.")


def baixar_state_of_data(dest_dir: Optional[Path] = None, force: bool = False) -> Path:
    """
    Baixa todas as edições do State of Data (2021–2025) listadas em DATASETS.

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

    # Se já existem CSVs e não for force, não baixa de novo
    if not force and any(dest_dir.glob("*.csv")):
        print(f"[StateOfData] Arquivos já encontrados em {dest_dir}, pulando download.")
        return dest_dir

    for slug, prefixo in DATASETS:
        try:
            baixar_dataset(slug, prefixo, dest_dir)
        except Exception as e:
            print(f"[StateOfData] ERRO ao baixar '{slug}': {e}")

    print(f"[StateOfData] Concluído. Arquivos em: {dest_dir.resolve()}")
    return dest_dir


if __name__ == "__main__":
    baixar_state_of_data()
