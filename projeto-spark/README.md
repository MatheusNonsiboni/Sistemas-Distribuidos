# Análise Global de Mudanças Climáticas — Spark Big Data
**Disciplina:** Sistemas Distribuídos 2026  
**Stack:** Docker · PySpark 3.5 · Jupyter Lab · MLlib

---

## Estrutura do Projeto

```
spark-climate/
├── docker-compose.yml          # Cluster: 1 Master + 2 Workers + Jupyter
├── setup.sh                    # Script de setup inicial
├── data/                       # ← Coloque os CSVs aqui
│   ├── GlobalLandTemperaturesByCity.csv
│   └── owid-co2-data.csv
├── notebooks/
│   ├── analise_climatica.py    # Pipeline principal (8 perguntas)
│   └── visualizacoes.py        # Gráficos com matplotlib
└── output/                     # Resultados gerados pelo Spark
    ├── p1_decadal_avg/
    ├── p2_hottest_years/
    ├── ...
    ├── dataset_final.parquet
    └── graficos/
```

---

## Passo a Passo

### 1. Baixar os datasets

| Dataset | Link | Arquivo |
|---------|------|---------|
| Temperaturas Berkeley Earth | [Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data) | `GlobalLandTemperaturesByCity.csv` |
| Emissões CO₂ | [GitHub owid](https://github.com/owid/co2-data) | `owid-co2-data.csv` |

Salve ambos em `./data/`.

### 2. Subir o cluster

```bash
chmod +x setup.sh && ./setup.sh
docker-compose up -d
```

### 3. Acessar o Jupyter Lab

Abra `http://localhost:8888` no navegador.  
Token: aparece no log do container (`docker logs jupyter-spark`).

### 4. Executar o pipeline

No Jupyter Lab, abra um terminal e rode:

```bash
cd /home/jovyan/work
python analise_climatica.py
```

Depois gere os gráficos:

```bash
pip install matplotlib seaborn
python visualizacoes.py
```

### 5. Monitorar no Spark UI

- **Master UI:** http://localhost:8080
- **Application UI (DAG/Stages):** http://localhost:4040

---

## Arquitetura do Cluster

| Componente | Configuração |
|-----------|-------------|
| Spark Master | 1 nó — porta 7077 |
| Spark Worker 1 | 2 cores · 2 GB RAM |
| Spark Worker 2 | 2 cores · 2 GB RAM |
| Driver (Jupyter) | 1 GB RAM |
| Spark Version | 3.5 |

---

## ETL — Limpeza Realizada

| Etapa | Técnica PySpark | Motivo |
|-------|----------------|--------|
| Remoção de nulos | `dropna(subset=["AverageTemperature"])` | Cidades antigas sem dados |
| Conversão de tipos | `to_date()` + `year()` / `month()` | Permitir `groupBy` por período |
| Filtro de incerteza | `filter(col < 1.5)` | Medições imprecisas distorcem médias |
| Filtro de período | `filter(Year >= 1900)` | Dados pré-1900 têm alta incerteza |
| Limpeza de coords | `regexp_replace` + sinal negativo p/ S e W | Necessário para análise geográfica |
| Normalização países | `trim()` + `regexp_replace()` | Chave do Join com CO₂ |
| Cache | `.cache()` após limpeza | Evita releitura do CSV em cada query |

---

## Perguntas e Técnicas

| # | Pergunta | Técnica Spark |
|---|---------|--------------|
| 1 | Temperatura por década | `groupBy` · `avg` · `Window` |
| 2 | Anos mais quentes por continente | `rank()` Window Function |
| 3 | Cidades mais instáveis | `stddev()` + filtro de registros |
| 4 | Correlação Min × Max tropicais | MLlib `Correlation` |
| 5 | Qualidade dos dados | `count` + threshold dinâmico |
| 6 | CO₂ × Temperatura (Join) | `join` + MLlib `Correlation` |
| 7 | Aceleração térmica | `lag()` Window Function |
| 8 | Previsão de temperatura | MLlib `LinearRegression` |

---

## Dicionário do Parquet Final

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `dt` | Date | Data da medição (YYYY-MM-DD) |
| `Year` | Int | Ano extraído de `dt` |
| `Month` | Int | Mês extraído de `dt` |
| `City` | String | Nome da cidade |
| `Country` | String | Nome do país (normalizado) |
| `AverageTemperature` | Double | Temperatura média mensal (°C) |
| `AverageTemperatureUncertainty` | Double | Margem de erro (°C) |
| `Lat` | Double | Latitude numérica (S negativo) |
| `Lon` | Double | Longitude numérica (W negativo) |
