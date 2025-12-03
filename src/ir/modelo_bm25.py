"""
Modelo de Recuperação da Informação usando BM25 sobre a base integrada.

Uso:
    python src/ir/modelo_bm25.py

O script vai carregar `data/processed/base_integrada.csv`,
construir um índice BM25 e abrir um loop de consulta interativo.
"""

from pathlib import Path
from typing import List, Dict

import pandas as pd
from rank_bm25 import BM25Okapi

BASE_FILE = Path("data/processed/base_integrada.csv")


class BM25Search:
    def __init__(self, corpus: List[str]):
        tokenized_corpus = [doc.lower().split() for doc in corpus]
        self.bm25 = BM25Okapi(tokenized_corpus)
        self.corpus = corpus

    def search(self, query: str, top_k: int = 10) -> List[Dict]:
        tokenized_query = query.lower().split()
        scores = self.bm25.get_scores(tokenized_query)
        indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]

        resultados = [
            {
                "rank": i + 1,
                "indice": idx,
                "score": float(scores[idx]),
                "texto": self.corpus[idx],
            }
            for i, idx in enumerate(indices)
        ]
        return resultados


def carregar_corpus(arquivo: Path = BASE_FILE) -> List[str]:
    if not arquivo.exists():
        raise FileNotFoundError(
            f"Base integrada não encontrada em {arquivo}. "
            "Rode primeiro os scripts de limpeza e integração."
        )

    df = pd.read_csv(arquivo)
    if "texto_ir" not in df.columns:
        raise ValueError("Coluna 'texto_ir' não encontrada na base integrada.")

    return df["texto_ir"].astype(str).tolist()


def loop_interativo(busca: BM25Search, top_k: int = 5) -> None:
    print("\n[BM25] Modelo pronto. Digite uma consulta (ou ENTER vazio para sair):\n")
    while True:
        query = input("consulta> ").strip()
        if not query:
            print("Encerrando.")
            break

        resultados = busca.search(query, top_k=top_k)
        print(f"\n[BM25] Top {top_k} resultados para: '{query}'\n")

        for r in resultados:
            print(f"Rank {r['rank']} | Score={r['score']:.4f}")
            print(r["texto"][:220].replace("\n", " ") + "...")
            print("-" * 80)
        print()


if __name__ == "__main__":
    corpus = carregar_corpus(BASE_FILE)
    print(f"[BM25] Corpus carregado com {len(corpus)} documentos.")
    bm25 = BM25Search(corpus)
    loop_interativo(bm25)
