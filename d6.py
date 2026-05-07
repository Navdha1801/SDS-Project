import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# =====================================================
# SETUP
# =====================================================
os.makedirs("d6_plots", exist_ok=True)

# =====================================================
# LOAD DATA
# =====================================================
df = pd.read_csv("d6_plots/compression_experiment.csv")

# =====================================================
# CLEAN DATA
# =====================================================
df = df.dropna()

# =====================================================
# CONVERT JOULES → MICROJOULES
# =====================================================
df["energy_uJ"] = df["mean_energy"] * 1_000_000

# =====================================================
# OPERATIONS
# =====================================================
operations = df["operation"].unique()

# =====================================================
# PLOT 1: COMPRESSION vs ENERGY
# =====================================================
for op in operations:

    plt.figure(figsize=(8,6))

    subset = df[
        df["operation"] == op
    ].sort_values("compression")

    x = subset["compression"].astype(str).tolist()
    y = subset["energy_uJ"].tolist()

    plt.plot(
        x,
        y,
        marker='o',
        linewidth=2
    )

    plt.title(f"Compression vs Energy ({op})")
    plt.xlabel("Compression Type")
    plt.ylabel("Energy (µJ)")

    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d6_plots/{op}_energy_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 2: COMPRESSION vs TIME
# =====================================================
for op in operations:

    plt.figure(figsize=(8,6))

    subset = df[
        df["operation"] == op
    ].sort_values("compression")

    x = subset["compression"].astype(str).tolist()
    y = subset["mean_time"].tolist()

    plt.plot(
        x,
        y,
        marker='o',
        linewidth=2
    )

    plt.title(f"Compression vs Time ({op})")
    plt.xlabel("Compression Type")
    plt.ylabel("Time (seconds)")

    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d6_plots/{op}_time.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 3: PARQUET-ONLY COMPARISON
# =====================================================
parquet_df = df[
    df["compression"].str.contains("PARQUET")
]

for op in parquet_df["operation"].unique():

    plt.figure(figsize=(8,6))

    subset = parquet_df[
        parquet_df["operation"] == op
    ].sort_values("compression")

    x = subset["compression"].astype(str).tolist()
    y = subset["energy_uJ"].tolist()

    plt.plot(
        x,
        y,
        marker='o',
        linewidth=2
    )

    plt.title(f"Parquet Compression Comparison ({op})")
    plt.xlabel("Codec")
    plt.ylabel("Energy (µJ)")

    plt.grid(True)
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d6_plots/{op}_parquet_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 4: ENERGY HEATMAP
# =====================================================
pivot_energy = df.pivot_table(
    index="compression",
    columns="operation",
    values="energy_uJ"
)

plt.figure(figsize=(10,6))

sns.heatmap(
    pivot_energy,
    annot=True,
    cmap="magma",
    fmt=".2f"
)

plt.title("Energy Heatmap (Compression vs Operation)")
plt.tight_layout()

# SAVE
plt.savefig(
    "d6_plots/heatmap_energy_microjoules.png",
    dpi=300
)

plt.close()

# =====================================================
# PLOT 5: TIME HEATMAP
# =====================================================
pivot_time = df.pivot_table(
    index="compression",
    columns="operation",
    values="mean_time"
)

plt.figure(figsize=(10,6))

sns.heatmap(
    pivot_time,
    annot=True,
    cmap="viridis",
    fmt=".2f"
)

plt.title("Time Heatmap (Compression vs Operation)")
plt.tight_layout()

# SAVE
plt.savefig(
    "d6_plots/heatmap_time.png",
    dpi=300
)

plt.close()

print("\n✅ All plots saved in d6_plots/")  