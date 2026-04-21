import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# -----------------------------
# SETUP
# -----------------------------
os.makedirs("d6_plots", exist_ok=True)

df = pd.read_csv("d6_plots/compression_results.csv")

# Clean
df = df.dropna()

operations = df["operation"].unique()

# -----------------------------
# PLOT 1: Compression vs Energy
# -----------------------------
for op in operations:
    plt.figure()

    subset = df[df["operation"] == op].sort_values("format")

    x = subset["format"].astype(str).tolist()
    y = subset["energy"].tolist()

    plt.plot(x, y, marker='o')
    plt.title(f"Compression vs Energy ({op})")
    plt.xlabel("Compression Type")
    plt.ylabel("Energy (Joules)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(f"d6_plots/{op}_energy.png")
    plt.close()

# -----------------------------
# PLOT 2: Compression vs Time
# -----------------------------
for op in operations:
    plt.figure()

    subset = df[df["operation"] == op].sort_values("format")

    x = subset["format"].astype(str).tolist()
    y = subset["time"].tolist()

    plt.plot(x, y, marker='o')
    plt.title(f"Compression vs Time ({op})")
    plt.xlabel("Compression Type")
    plt.ylabel("Time (seconds)")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(f"d6_plots/{op}_time.png")
    plt.close()

# -----------------------------
# PLOT 3: Parquet-only Comparison
# -----------------------------
parquet_df = df[df["format"].str.contains("PARQUET")]

for op in parquet_df["operation"].unique():
    plt.figure()

    subset = parquet_df[parquet_df["operation"] == op].sort_values("format")

    x = subset["format"].astype(str).tolist()
    y = subset["energy"].tolist()

    plt.plot(x, y, marker='o')
    plt.title(f"Parquet Compression Comparison ({op})")
    plt.xlabel("Codec")
    plt.ylabel("Energy (Joules)")
    plt.grid(True)
    plt.tight_layout()

    plt.savefig(f"d6_plots/{op}_parquet.png")
    plt.close()

# -----------------------------
# PLOT 4: Heatmap (Energy)
# -----------------------------
pivot_energy = df.pivot_table(
    index="format",
    columns="operation",
    values="energy"
)

plt.figure(figsize=(10,6))
sns.heatmap(pivot_energy, annot=True)
plt.title("Energy Heatmap (Compression vs Operation)")
plt.tight_layout()
plt.savefig("d6_plots/heatmap_energy.png")
plt.close()

# -----------------------------
# PLOT 5: Heatmap (Time)
# -----------------------------
pivot_time = df.pivot_table(
    index="format",
    columns="operation",
    values="time"
)

plt.figure(figsize=(10,6))
sns.heatmap(pivot_time, annot=True)
plt.title("Time Heatmap (Compression vs Operation)")
plt.tight_layout()
plt.savefig("d6_plots/heatmap_time.png")
plt.close()

print("\n✅ All plots saved in d6_plots/ folder!")