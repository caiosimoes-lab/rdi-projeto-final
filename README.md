# Projeto RDI – State of Data Brazil (2021–2025) + Mercado de Trabalho

Este repositório segue a estrutura exigida pelo trabalho final de Recuperação da Informação.

## Objetivo

Explorar as edições da pesquisa **State of Data Brazil (2021 a 2025)** e integrar com uma fonte externa
de vagas de trabalho em Ciência de Dados / Analytics, coletadas automaticamente via API/Web Scraping.
A partir disso, aplicar técnicas de limpeza, padronização, indexação e **Recuperação da Informação**
(ex.: BM25, modelo vetorial) e produzir um relatório analítico com visualizações e interpretações.

## Estrutura de Pastas

```text
data/
  raw/         # Dados brutos baixados via código (State of Data, API externa)
  external/    # Dados externos de apoio (se necessário)
  processed/   # Dados limpos / integrados prontos para análise

src/
  coleta/      # Scripts de coleta (State of Data + fonte externa)
  limpeza/     # Scripts de limpeza, padronização e integração das bases
  ir/          # Scripts de modelos de recuperação da informação (BM25, vetorial etc.)

notebooks/
  analise.ipynb  # Notebook principal de análise exploratória + testes dos modelos IR

docs/
  relatorio.qmd  # Relatório final (Quarto) ou rascunho em notebook
  relatorio.pdf  # Versão em PDF gerada a partir do .qmd ou .ipynb
```

## Como reproduzir o projeto

1. Crie e ative um ambiente virtual (opcional, mas recomendado):
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   .venv\Scripts\activate   # Windows
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. **Coleta de dados**
   ```bash
   # Coleta State of Data Brazil (Kaggle)
   python src/coleta/coleta_state_of_data.py

   # Coleta de vagas (API RemoteOK – exemplo de fonte externa)
   python src/coleta/coleta_externa.py
   ```

4. **Limpeza e integração**
   ```bash
   python src/limpeza/limpeza_state_of_data.py
   python src/limpeza/integracao_bases.py
   ```

5. **Modelos de Recuperação da Informação**
   - Use o notebook `notebooks/analise.ipynb` para:
     - carregar os dados processados
     - ajustar o modelo BM25 / vetorial
     - testar consultas
     - gerar gráficos e tabelas
   - Os scripts de apoio estão em `src/ir/`.

6. **Relatório final**
   - Edite `docs/relatorio.qmd` ou o próprio `notebooks/analise.ipynb`.
   - Exporte para PDF e salve como `docs/relatorio.pdf`.

## Observação

Este template traz a **estrutura completa** e **arquivos-base** para você adaptar,
completar o código, rodar os experimentos e escrever o relatório conforme as
instruções da disciplina.
