import matplotlib
matplotlib.use('Agg')  # backend sem display, necessário para rodar como script

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
import numpy as np
import glob, os

OUTPUT_DIR = "/opt/output"
GRAFICOS_DIR = os.path.join(OUTPUT_DIR, "graficos")
os.makedirs(GRAFICOS_DIR, exist_ok=True)

def ler_csv_spark(pasta):
    """Lê CSV gerado pelo Spark (part-*.csv)."""
    arquivos = glob.glob(os.path.join(OUTPUT_DIR, pasta, "part-*.csv"))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em: {OUTPUT_DIR}/{pasta}/part-*.csv")
    return pd.concat([pd.read_csv(f) for f in arquivos])

# GRÁFICO 1 — Curva de Aquecimento Global por Década

df_p1 = ler_csv_spark("p1_decadal_avg")
df_p1 = df_p1.sort_values("Decade")

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(df_p1["Decade"], df_p1["AvgTemp_Decade"],
        marker="o", linewidth=2.5, color="#E63946", markersize=7)
ax.fill_between(df_p1["Decade"], df_p1["AvgTemp_Decade"],
                df_p1["AvgTemp_Decade"].min() - 0.5,
                alpha=0.15, color="#E63946")
ax.set_title("Temperatura Média Global por Década\n(Dados: Berkeley Earth / Kaggle)",
             fontsize=14, fontweight="bold", pad=15)
ax.set_xlabel("Década", fontsize=12)
ax.set_ylabel("Temperatura Média (°C)", fontsize=12)
ax.xaxis.set_major_locator(ticker.MultipleLocator(10))
ax.grid(axis="y", linestyle="--", alpha=0.4)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.savefig(f"{GRAFICOS_DIR}/g1_aquecimento_global.png", dpi=150)
plt.close()
print("Gráfico 1 salvo.")

# GRÁFICO 2 — Dispersão CO2 × Temperatura (Pergunta 6)

df_p6 = ler_csv_spark("p6_co2_temp_join")
df_p6 = df_p6.dropna(subset=["co2", "AvgTemp_Country"])
df_p6 = df_p6[df_p6["co2"] < df_p6["co2"].quantile(0.98)]

fig, ax = plt.subplots(figsize=(10, 6))
sc = ax.scatter(df_p6["co2"], df_p6["AvgTemp_Country"],
                alpha=0.3, s=10, c=df_p6["Year"],
                cmap="YlOrRd", rasterized=True)
plt.colorbar(sc, ax=ax, label="Ano")

# Linha de tendência — usando numpy diretamente (pd.np removido no pandas 2.0)
m, b = np.polyfit(df_p6["co2"], df_p6["AvgTemp_Country"], 1)
x_range = np.linspace(df_p6["co2"].min(), df_p6["co2"].max(), 100)
ax.plot(x_range, m * x_range + b, color="black", linewidth=2,
        label=f"Tendência (y={m:.5f}x+{b:.2f})")

ax.set_title("Emissões de CO₂ × Temperatura Média por País/Ano\n(Correlação de Pearson)",
             fontsize=13, fontweight="bold")
ax.set_xlabel("Emissões CO₂ (Mt)", fontsize=12)
ax.set_ylabel("Temperatura Média (°C)", fontsize=12)
ax.legend(fontsize=10)
ax.spines[["top", "right"]].set_visible(False)
plt.tight_layout()
plt.savefig(f"{GRAFICOS_DIR}/g2_co2_vs_temperatura.png", dpi=150)
plt.close()
print("Gráfico 2 salvo.")

# GRÁFICO 3 — Top 15 Cidades Mais Instáveis (Pergunta 3)

df_p3 = ler_csv_spark("p3_unstable_cities").head(15)

fig, ax = plt.subplots(figsize=(10, 6))
colors = sns.color_palette("Reds_r", len(df_p3))
ax.barh(df_p3["City"] + " (" + df_p3["Country"] + ")",
        df_p3["StdDev_Temp"], color=colors)
ax.set_title("Cidades com Maior Instabilidade Climática\n(Desvio Padrão da Temperatura — Último Século)",
             fontsize=12, fontweight="bold")
ax.set_xlabel("Desvio Padrão (°C)", fontsize=11)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="x", linestyle="--", alpha=0.4)
plt.tight_layout()
plt.savefig(f"{GRAFICOS_DIR}/g3_cidades_instaveis.png", dpi=150)
plt.close()
print("Gráfico 3 salvo.")

# GRÁFICO 4 — Previsão de Temperatura (Pergunta 8)

df_p8 = ler_csv_spark("p8_forecast")

fig, ax = plt.subplots(figsize=(8, 5))
ax.bar(df_p8["YearDouble"].astype(int), df_p8["prediction"],
       color="#457B9D", alpha=0.85)
ax.set_title("Previsão de Temperatura — Moscou, Russia\n(Regressão Linear — Spark MLlib)",
             fontsize=12, fontweight="bold")
ax.set_xlabel("Ano", fontsize=11)
ax.set_ylabel("Temperatura Prevista (°C)", fontsize=11)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="y", linestyle="--", alpha=0.4)
plt.tight_layout()
plt.savefig(f"{GRAFICOS_DIR}/g4_previsao_temp.png", dpi=150)
plt.close()
print("Gráfico 4 salvo.")

print(f"\nTodos os gráficos salvos em: {GRAFICOS_DIR}")
