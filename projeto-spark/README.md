# Análise Global de Mudanças Climáticas — Spark Big Data
**Disciplina:** Sistemas Distribuídos 2026  
**Stack:** Docker · PySpark 3.5.1 · Jupyter Lab · MLlib

## Autores 

Implementação desenvolvida por mim; relatório e análise de resultados desenvolvidos em conjunto com Livia Kouketsu da Silva.

## Estrutura do Projeto

```
projeto-spark/
├── dados/                          # CSVs de entrada (não versionados)
│   ├── GlobalLandTemperaturesByCity.csv
│   └── owid-co2-data.csv
├── notebooks/
│   ├── analise_climatica.py        # Pipeline principal — 8 perguntas
│   └── visualizacoes.py            # Gráficos com matplotlib/seaborn
├── output/                         # Resultados gerados pelo Spark
│   ├── p1_decadal_avg/
│   ├── p2_hottest_years/
│   ├── p3_unstable_cities/
│   ├── p6_co2_temp_join/
│   ├── p7_thermal_accel/
│   ├── p8_forecast/
│   ├── dataset_final.parquet
│   └── graficos/
├── docker-compose.yml              # Cluster Master + Worker-1 + Jupyter
└── docker-compose-worker.yml       # Worker-2 (PC da colega)
```

## Datasets

| Dataset | Fonte | Arquivo |
|---------|-------|---------|
| Temperaturas Berkeley Earth | [Kaggle](https://www.kaggle.com/datasets/berkeleyearth/climate-change-earth-surface-temperature-data) | `GlobalLandTemperaturesByCity.csv` |
| Emissões de CO₂ | [Our World in Data](https://github.com/owid/co2-data) | `owid-co2-data.csv` |

Salve os dois arquivos em `./dados/` antes de subir o cluster.

## Arquitetura do Cluster

| Componente | Imagem Docker | Configuração |
|-----------|---------------|-------------|
| Spark Master | `quay.io/jupyter/pyspark-notebook:spark-3.5.1` | porta 7077 · UI: 8080 |
| Spark Worker-1 | `quay.io/jupyter/pyspark-notebook:spark-3.5.1` | 2 cores · 2 GB RAM |
| Spark Worker-2 | `quay.io/jupyter/pyspark-notebook:spark-3.5.1` | 2 cores · 2 GB RAM (PC externo) |
| Jupyter Driver | `quay.io/jupyter/pyspark-notebook:spark-3.5.1` | porta 8888 · UI: 4040 |

A mesma imagem é usada em todos os containers para garantir compatibilidade de versão do Python e do PySpark entre Driver e Executors.

## Passo a Passo

### 1. Subir o cluster (PC Master)

```powershell
docker-compose up -d
```

### 2. Subir o Worker externo (PC da colega)

Edite o IP em `docker-compose-worker.yml` para o IP atual do PC Master e rode:

```powershell
docker-compose -f docker-compose-worker.yml up -d
```

### 3. Confirmar o cluster

Acesse `http://localhost:8080` — devem aparecer **3 Workers** com status ALIVE.

### 4. Obter o token do Jupyter

```powershell
docker logs jupyter-spark 2>&1 | Select-String "token="
```

Acesse `http://localhost:8888` com o token exibido.

### 5. Executar o pipeline

No terminal do Jupyter Lab:

```bash
cd work
python analise_climatica.py
```

### 6. Gerar os gráficos

```bash
python visualizacoes.py
```

Os gráficos são salvos em `output/graficos/`.

### 7. Monitorar no Spark UI

| Interface | URL | Disponível |
|-----------|-----|-----------|
| Master UI (Workers) | http://localhost:8080 | Sempre |
| Application UI (DAG/Stages) | http://localhost:4040 | Somente durante execução |

## ETL — Limpeza Realizada

| Etapa | Técnica PySpark | Motivo |
|-------|----------------|--------|
| Remoção de nulos | `dropna(subset=["AverageTemperature"])` | Cidades antigas sem dados |
| Conversão de tipos | `to_date()` + `year()` / `month()` | Permitir `groupBy` por período |
| Filtro de incerteza | `filter(col < 1.5)` | Medições imprecisas distorcem médias |
| Filtro de período | `filter(Year >= 1900)` | Dados pré-1900 têm alta incerteza |
| Limpeza de coords | `regexp_replace` + sinal negativo p/ S e W | Filtros geográficos da Pergunta 4 |
| Normalização países | `trim()` + `regexp_replace()` | Chave do JOIN com CO₂ |
| Cache | `.cache()` após limpeza | Evita releitura do CSV em cada query |

## Perguntas e Técnicas Spark

| # | Pergunta | Técnica Principal |
|---|---------|------------------|
| 1 | Temperatura média por década | `groupBy` + `avg` duplo |
| 2 | 10 anos mais quentes por continente | `rank()` Window Function |
| 3 | Cidades com maior instabilidade | `stddev()` + filtro de registros |
| 4 | Correlação Min × Max (trópicos) | MLlib `Correlation` + `VectorAssembler` |
| 5 | Qualidade dos dados | threshold dinâmico + `count` |
| 6 | CO₂ × Temperatura (Join) | `join` + MLlib `Correlation` |
| 7 | Aceleração térmica por país | `lag()` Window Function |
| 8 | Previsão de temperatura | MLlib `LinearRegression` |

## Dicionário do Parquet Final

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `dt` | Date | Data da medição original (YYYY-MM-DD) |
| `Year` | Int | Ano extraído da coluna `dt` |
| `Month` | Int | Mês extraído da coluna `dt` |
| `City` | String | Nome da cidade (grafia original do dataset) |
| `Country` | String | Nome do país normalizado (trim + regexp_replace) |
| `AverageTemperature` | Double | Temperatura média mensal em graus Celsius |
| `AverageTemperatureUncertainty` | Double | Margem de erro da medição em graus Celsius |
| `Lat` | Double | Latitude numérica — Sul é negativo (ex: -23.5) |
| `Lon` | Double | Longitude numérica — Oeste é negativo (ex: -46.6) |
