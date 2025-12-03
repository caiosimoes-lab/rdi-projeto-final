from pathlib import Path
from typing import List

import pandas as pd
from rank_bm25 import BM25Okapi


class BM25Search:
    def __init__(self, corpus: List[str]):
        tokenized_corpus = [doc.lower().split() for doc in corpus]
        self.bm25 = BM25Okapi(tokenized_corpus)
        self.corpus = corpus

    def search(self, query: str, top_k: int = 10):
        tokenized_query = query.lower().split()
        scores = self.bm25.get_scores(tokenized_query)
        indices = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        resultados = [
            {"rank": i + 1, "indice": idx, "score": float(scores[idx]), "texto": self.corpus[idx]}
            for i, idx in enumerate(indices)
        ]
        return resultados


def carregar_corpus(arquivo: str = "data/processed/base_integrada.csv") -> list:
    df = pd.read_csv(arquivo)
    if "texto_ir" not in df.columns:
        raise ValueError("Coluna 'texto_ir' não encontrada na base integrada.")
    return df["texto_ir"].astype(str).tolist()


if __name__ == "__main__":
    base_file = "data/processed/base_integrada.csv"
    if not Path(base_file).exists():
        raise FileNotFoundError(
            f"{base_file} não encontrado. Rode primeiro o script de integração de bases."
        )

    corpus = carregar_corpus(base_file)
    bm25 = BM25Search(corpus)
    print("Modelo BM25 carregado. Exemplo de consulta:")
    query = "salário cientista de dados brasil remoto"
    resultados = bm25.search(query, top_k=5)
    for r in resultados:
        print(f"Rank {r['rank']} | Score={r['score']:.4f}")
        print(r["texto"][:200], "...")
        print("-" * 80)
