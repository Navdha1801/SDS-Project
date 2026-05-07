import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# =====================================================
# LOAD DATA
# =====================================================
df = pd.read_csv("d1_plots/format_experiment.csv")

# =====================================================
# CREATE OUTPUT FOLDER
# =====================================================
os.makedirs("d1_plots", exist_ok=True)

# =====================================================
# CLEAN DATA
# =====================================================
df = df.dropna()

# =====================================================
# CONVERT JOULES → MICROJOULES
# =====================================================
df["Energy_uJ"] = df["Energy"] * 1_000_000

# =====================================================
# DATASET SIZE ORDER
# =====================================================
size_map = {
    "Maldives": "Small",
    "Bosnia": "Medium",
    "Tanzania": "Large"
}

df["Size"] = df["Dataset"].map(size_map)

size_order = ["Small", "Medium", "Large"]

df["Size"] = pd.Categorical(
    df["Size"],
    categories=size_order,
    ordered=True
)

# =====================================================
# PLOT 1: ENERGY VS DATASET SIZE
# =====================================================
formats = df["Format"].unique()

for fmt in formats:

    plt.figure(figsize=(8,6))

    subset = df[df["Format"] == fmt]

    for op in subset["Operation"].unique():

        op_data = subset[
            subset["Operation"] == op
        ].sort_values("Size")

        x = op_data["Size"].astype(str).tolist()
        y = op_data["Energy_uJ"].tolist()

        plt.plot(
            x,
            y,
            marker='o',
            linewidth=2,
            label=op
        )

    plt.title(f"Energy Scaling - {fmt}")
    plt.xlabel("Dataset Size")
    plt.ylabel("Energy (µJ)")
    plt.legend()
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d1_plots/{fmt}_scaling_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 2: FORMAT COMPARISON
# =====================================================
datasets = df["Dataset"].unique()

for ds in datasets:

    plt.figure(figsize=(8,6))

    subset = df[df["Dataset"] == ds]

    for op in subset["Operation"].unique():

        op_data = subset[
            subset["Operation"] == op
        ]

        x = op_data["Format"].tolist()
        y = op_data["Energy_uJ"].tolist()

        plt.plot(
            x,
            y,
            marker='o',
            linewidth=2,
            label=op
        )

    plt.title(f"Format Comparison - {ds}")
    plt.xlabel("Format")
    plt.ylabel("Energy (µJ)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d1_plots/{ds}_comparison_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 3: HEATMAP
# =====================================================
pivot = df.pivot_table(
    index="Format",
    columns="Operation",
    values="Energy_uJ",
    aggfunc="mean"
)

plt.figure(figsize=(8,6))

sns.heatmap(
    pivot,
    annot=True,
    cmap="coolwarm",
    fmt=".2f"
)

plt.title("Energy Heatmap (µJ)")
plt.tight_layout()

# SAVE
plt.savefig(
    "d1_plots/energy_heatmap_microjoules.png",
    dpi=300
)

plt.close()

print("\n✅ All plots saved in d1_plots/")