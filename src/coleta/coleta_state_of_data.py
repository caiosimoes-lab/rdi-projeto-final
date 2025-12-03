import os
from pathlib import Path

try:
    import kagglehub
except ImportError:
    kagglehub = None


def baixar_state_of_data(dest_dir: str = "data/raw/state_of_data") -> None:
    """Baixa os arquivos da pesquisa State of Data Brazil (2021–2025) via kagglehub.

    É necessário ter o Kaggle configurado localmente com API token.
    """
    if kagglehub is None:
        raise ImportError(
            "O pacote 'kagglehub' não está instalado. "
            "Instale com `pip install kagglehub` e configure sua conta Kaggle."
        )

    print("Baixando dataset 'basedosdados/state-of-data-brazil' do Kaggle...")
    path = kagglehub.dataset_download("basedosdados/state-of-data-brazil")

    dest_path = Path(dest_dir)
    dest_path.mkdir(parents=True, exist_ok=True)

    for file in Path(path).glob("**/*"):
        if file.is_file():
            rel = file.name
            target = dest_path / rel
            print(f"Copiando {file} -> {target}")
            target.write_bytes(file.read_bytes())

    print("Download concluído. Arquivos salvos em:", dest_path.resolve())


if __name__ == "__main__":
    baixar_state_of_data()
