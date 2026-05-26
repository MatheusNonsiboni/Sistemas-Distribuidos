from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, DateType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import time
import os
os.environ["PYSPARK_PYTHON"] = "/opt/conda/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/bin/python3"
OUTPUT = "/opt/output"

# INICIALIZAÇÃO DO SPARK

spark = (SparkSession.builder
    .appName("ClimateAnalysis_SD2026")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.cores", "2")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "50")
    .config("spark.pyspark.python", "/opt/conda/bin/python3")          # ← novo
    .config("spark.pyspark.driver.python", "/opt/conda/bin/python3")   # ← novo
    .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/conda/bin/python3")
    .config("spark.sql.warehouse.dir", "/opt/output")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print("Spark iniciado:", spark.version)

# INGESTÃO DOS DADOS (Pergunta D — Pipeline de Ingestão)

PATH_TEMP = "/opt/dados/GlobalLandTemperaturesByCity.csv"
PATH_CO2  = "/opt/dados/owid-co2-data.csv"

df_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(PATH_TEMP))

df_co2_raw = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(PATH_CO2))

print("Linhas temperatura (bruto):", df_raw.count())
print("Schema:")
df_raw.printSchema()

# ETL — LIMPEZA E TRANSFORMAÇÕES

# A) Remover nulos em AverageTemperature
df_clean = df_raw.dropna(subset=["AverageTemperature"])

# B) Converter coluna dt para DateType + extrair Ano e Mês
df_clean = (df_clean
    .withColumn("dt", F.to_date(F.col("dt"), "yyyy-MM-dd"))
    .withColumn("Year",  F.year("dt"))
    .withColumn("Month", F.month("dt")))

# C) Filtrar incerteza alta (Pergunta 5 — filtro de qualidade)
df_clean = df_clean.filter(F.col("AverageTemperatureUncertainty") < 1.5)

# D) Filtrar período confiável (1900 em diante)
df_clean = df_clean.filter(F.col("Year") >= 1900)

# E) Limpar coordenadas (remover N/S/E/W e ajustar sinal)
df_clean = (df_clean
    .withColumn("Lat",
        F.when(F.col("Latitude").endswith("S"),
               -F.regexp_replace("Latitude", "[NS]", "").cast(DoubleType()))
         .otherwise(F.regexp_replace("Latitude", "[NS]", "").cast(DoubleType())))
    .withColumn("Lon",
        F.when(F.col("Longitude").endswith("W"),
               -F.regexp_replace("Longitude", "[EW]", "").cast(DoubleType()))
         .otherwise(F.regexp_replace("Longitude", "[EW]", "").cast(DoubleType()))))

# F) Normalizar nomes de países (para o Join com CO2)
df_clean = (df_clean
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("Country",
        F.regexp_replace(F.col("Country"), "(?i)united states", "United States"))
    .withColumn("Country",
        F.regexp_replace(F.col("Country"), "(?i)united kingdom", "United Kingdom")))

# Cache após limpeza — reutilizado em todas as perguntas
t0 = time.time()
df_clean.cache()
df_clean.count()   # força a materialização
t_cache = time.time() - t0
print(f"Cache materializado em {t_cache:.1f}s")
print("Linhas após limpeza:", df_clean.count())

# PERGUNTA 1 — Média Móvel de Temperatura por Década

print("\n=== PERGUNTA 1: Temperatura Média por Década ===")

df_p1 = (df_clean
    .groupBy("Year")
    .agg(F.avg("AverageTemperature").alias("AvgTemp_Year"))
    .withColumn("Decade", (F.col("Year") / 10).cast(IntegerType()) * 10)
    .groupBy("Decade")
    .agg(F.avg("AvgTemp_Year").alias("AvgTemp_Decade"))
    .orderBy("Decade"))

df_p1.show(20)
df_p1.coalesce(1).write.mode("overwrite").csv("/opt/output/p1_decadal_avg", header=True)

# PERGUNTA 2 — 10 Anos Mais Quentes por Continente (últimos 50 anos)

print("\n=== PERGUNTA 2: Anos Mais Quentes por Continente ===")

continente_map = {
    "Brazil": "América do Sul", "Argentina": "América do Sul",
    "Colombia": "América do Sul", "Peru": "América do Sul",
    "Chile": "América do Sul", "Venezuela": "América do Sul",
    "United States": "América do Norte", "Canada": "América do Norte",
    "Mexico": "América do Norte",
    "Germany": "Europa", "France": "Europa", "United Kingdom": "Europa",
    "Italy": "Europa", "Spain": "Europa", "Russia": "Europa",
    "China": "Ásia", "India": "Ásia", "Japan": "Ásia",
    "Indonesia": "Ásia", "Pakistan": "Ásia",
    "Nigeria": "África", "Ethiopia": "África", "Egypt": "África",
    "South Africa": "África", "Kenya": "África",
    "Australia": "Oceania", "New Zealand": "Oceania"
}

cont_rows = [(k, v) for k, v in continente_map.items()]
df_cont = spark.createDataFrame(cont_rows, ["Country", "Continent"])

df_p2 = (df_clean
    .filter(F.col("Year") >= (2013 - 50))
    .join(df_cont, "Country", "inner")
    .groupBy("Continent", "Year")
    .agg(F.avg("AverageTemperature").alias("AvgTemp"))
    .withColumn("rank",
        F.rank().over(
            Window.partitionBy("Continent").orderBy(F.desc("AvgTemp"))))
    .filter(F.col("rank") <= 10)
    .orderBy("Continent", "rank"))

df_p2.show(50)
df_p2.coalesce(1).write.mode("overwrite").csv("/opt/output/p2_hottest_years", header=True)
# PERGUNTA 3 — Cidades com Maior Instabilidade Climática (último século)

print("\n=== PERGUNTA 3: Cidades com Maior Desvio Padrão ===")

df_p3 = (df_clean
    .filter(F.col("Year") >= 1913)
    .groupBy("City", "Country")
    .agg(
        F.stddev("AverageTemperature").alias("StdDev_Temp"),
        F.count("*").alias("n_records"))
    .filter(F.col("n_records") >= 100)
    .orderBy(F.desc("StdDev_Temp"))
    .limit(20))

df_p3.show(20)
df_p3.coalesce(1).write.mode("overwrite").csv("/opt/output/p3_unstable_cities", header=True)
# PERGUNTA 4 — Correlação Temp Mínima × Temp Máxima (Zonas Tropicais)

print("\n=== PERGUNTA 4: Correlação Min/Max em Zonas Tropicais ===")

df_tropical = (df_clean
    .filter((F.col("Lat") >= -23.5) & (F.col("Lat") <= 23.5))
    .groupBy("Country", "Year")
    .agg(
        F.min("AverageTemperature").alias("TempMin"),
        F.max("AverageTemperature").alias("TempMax")))

assembler = VectorAssembler(inputCols=["TempMin", "TempMax"], outputCol="features")
df_vec = assembler.transform(df_tropical.dropna()).select("features")
corr_matrix = Correlation.corr(df_vec, "features").head()
pearson = corr_matrix[0][0, 1]
print(f"Correlação de Pearson (TempMin × TempMax - Trópicos): {pearson:.4f}")

# PERGUNTA 5 — Qualidade de Dados: Registros com Alta Incerteza

print("\n=== PERGUNTA 5: Análise de Qualidade de Dados ===")

media_incerteza = df_raw.select(
    F.avg("AverageTemperatureUncertainty")).first()[0]

threshold = media_incerteza * 0.10
print(f"Média histórica de incerteza: {media_incerteza:.4f}°C")
print(f"Threshold (10%): {threshold:.4f}°C")

df_baixa_qualidade = df_raw.filter(
    F.col("AverageTemperatureUncertainty") > threshold)

total = df_raw.count()
baixa_q = df_baixa_qualidade.count()
print(f"Registros com incerteza > 10% da média: {baixa_q} ({100*baixa_q/total:.1f}%)")
print(f"Registros de qualidade aceitável: {total - baixa_q} ({100*(total-baixa_q)/total:.1f}%)")

# PERGUNTA 6 — Correlação CO2 × Temperatura por País (Join)

print("\n=== PERGUNTA 6: Correlação CO2 × Temperatura (Join) ===")

df_co2 = (df_co2_raw
    .select("country", "year", "co2", "co2_per_capita")
    .filter(F.col("year") >= 1960)
    .filter(~F.col("country").isin(["World", "Asia", "Europe",
                                    "Africa", "North America",
                                    "South America", "Oceania",
                                    "European Union (27)"]))
    .dropna(subset=["co2"])
    .withColumnRenamed("country", "Country_co2")
    .withColumnRenamed("year", "Year_co2"))

df_temp_anual = (df_clean
    .filter(F.col("Year") >= 1960)
    .groupBy("Country", "Year")
    .agg(F.avg("AverageTemperature").alias("AvgTemp_Country")))

df_temp_anual = df_temp_anual.withColumn(
    "Country_join", F.trim(F.lower(F.col("Country"))))
df_co2 = df_co2.withColumn(
    "Country_join", F.trim(F.lower(F.col("Country_co2"))))

df_joined = (df_temp_anual
    .join(df_co2,
          (df_temp_anual["Country_join"] == df_co2["Country_join"]) &
          (df_temp_anual["Year"] == df_co2["Year_co2"]),
          "inner")
    .select("Country", "Year", "AvgTemp_Country", "co2", "co2_per_capita"))

df_joined.cache()
print("Linhas após join:", df_joined.count())

assembler_co2 = VectorAssembler(
    inputCols=["co2", "AvgTemp_Country"], outputCol="features")
df_vec_co2 = assembler_co2.transform(df_joined.dropna()).select("features")
corr_co2 = Correlation.corr(df_vec_co2, "features").head()[0][0, 1]
print(f"Correlação de Pearson (CO2 × Temperatura): {corr_co2:.4f}")

df_joined.coalesce(1).write.mode("overwrite").csv("/opt/output/p6_co2_temp_join", header=True)

# PERGUNTA 7 — Ranking de Aceleração Térmica (Window Functions)

print("\n=== PERGUNTA 7: Países com Maior Aceleração Térmica ===")

df_decadal = (df_clean
    .withColumn("Decade", (F.col("Year") / 10).cast(IntegerType()) * 10)
    .groupBy("Country", "Decade")
    .agg(F.avg("AverageTemperature").alias("AvgTemp_Decade")))

w_country = Window.partitionBy("Country").orderBy("Decade")

df_p7 = (df_decadal
    .withColumn("AvgTemp_PrevDecade",
        F.lag("AvgTemp_Decade", 1).over(w_country))
    .withColumn("ThermalAccel",
        F.col("AvgTemp_Decade") - F.col("AvgTemp_PrevDecade"))
    .filter(F.col("Decade") == 2010)
    .dropna(subset=["ThermalAccel"])
    .orderBy(F.desc("ThermalAccel"))
    .limit(10))

df_p7.show(10)
df_p7.coalesce(1).write.mode("overwrite").csv("/opt/output/p7_thermal_accel", header=True)
# PERGUNTA 8 — Previsão de Temperatura via Regressão Linear (MLlib)

print("\n=== PERGUNTA 8: Previsão com Regressão Linear (MLlib) ===")

CIDADE_ALVO  = "Moscow"
PAIS_ALVO    = "Russia"

df_cidade = (df_clean
    .filter((F.col("City") == CIDADE_ALVO) &
            (F.col("Country") == PAIS_ALVO))
    .filter(F.col("Year") >= (2013 - 50))
    .groupBy("Year")
    .agg(F.avg("AverageTemperature").alias("AvgTemp"))
    .withColumn("YearDouble", F.col("Year").cast(DoubleType())))

assembler_ml = VectorAssembler(inputCols=["YearDouble"], outputCol="features")
df_ml = assembler_ml.transform(df_cidade)

lr = LinearRegression(featuresCol="features", labelCol="AvgTemp")
model_lr = lr.fit(df_ml)

anos_futuros = [(float(y),) for y in range(2014, 2034)]
df_future = spark.createDataFrame(anos_futuros, ["YearDouble"])
df_future = assembler_ml.transform(df_future)
predicoes = model_lr.transform(df_future).select("YearDouble", "prediction")

print(f"Coeficiente (slope): {model_lr.coefficients[0]:.6f}°C/ano")
print(f"Intercepto: {model_lr.intercept:.4f}")
print(f"R²: {model_lr.summary.r2:.4f}")
print("\nPrevisões:")
predicoes.show()

predicoes.coalesce(1).write.mode("overwrite").csv("/opt/output/p8_forecast", header=True)

# CACHE BENCHMARK — Comparativo com/sem cache

print("\n=== BENCHMARK: Com Cache vs Sem Cache ===")

t_start = time.time()
df_clean.unpersist()
df_clean.filter(F.col("Year") >= 1990).groupBy("Country").agg(
    F.avg("AverageTemperature")).count()
t_sem_cache = time.time() - t_start
print(f"Sem cache: {t_sem_cache:.2f}s")

df_clean.cache()
df_clean.count()
t_start = time.time()
df_clean.filter(F.col("Year") >= 1990).groupBy("Country").agg(
    F.avg("AverageTemperature")).count()
t_com_cache = time.time() - t_start
print(f"Com cache: {t_com_cache:.2f}s")
print(f"Ganho: {t_sem_cache/t_com_cache:.1f}x mais rápido")

# SAÍDA — Parquet (Data Lake Simulado)

print("\n=== Salvando Dataset Final em Parquet ===")

df_final = (df_clean
    .select("dt", "Year", "Month", "City", "Country",
            "AverageTemperature", "AverageTemperatureUncertainty",
            "Lat", "Lon")
    .filter(F.col("Year") >= 1960))

df_final.write.mode("overwrite").parquet("/opt/output/dataset_final.parquet")
print("Schema do Parquet final:")
df_final.printSchema()

spark.stop()
print("\n=== Pipeline concluído com sucesso! ===")
