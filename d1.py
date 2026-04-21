import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("data/results.csv")

# -----------------------------
# FIX 1: Clean data
# -----------------------------
df = df.dropna()

# -----------------------------
# FIX 2: Convert dataset → ordered size
# -----------------------------
size_map = {
    "Maldives": "Small",
    "Bosnia": "Medium",
    "Tanzania": "Large"
}

df["Size"] = df["Dataset"].map(size_map)

# Define order
size_order = ["Small", "Medium", "Large"]
df["Size"] = pd.Categorical(df["Size"], categories=size_order, ordered=True)

# -----------------------------
# PLOT 1: Energy vs Dataset Size
# -----------------------------
formats = df["Format"].unique()

for fmt in formats:
    plt.figure()

    subset = df[df["Format"] == fmt]

    for op in subset["Operation"].unique():
        op_data = subset[subset["Operation"] == op].sort_values("Size")

        # ✅ convert to list (CRITICAL FIX)
        x = op_data["Size"].astype(str).tolist()
        y = op_data["Energy"].tolist()

        plt.plot(x, y, marker='o', label=op)

    plt.title(f"Energy Scaling - {fmt}")
    plt.xlabel("Dataset Size")
    plt.ylabel("Energy (Joules)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{fmt}_scaling.png")
    plt.show()

# -----------------------------
# PLOT 2: Format Comparison per Dataset
# -----------------------------
datasets = df["Dataset"].unique()

for ds in datasets:
    plt.figure()

    subset = df[df["Dataset"] == ds]

    for op in subset["Operation"].unique():
        op_data = subset[subset["Operation"] == op]

        # ✅ FIX HERE ALSO
        x = op_data["Format"].tolist()
        y = op_data["Energy"].tolist()

        plt.plot(x, y, marker='o', label=op)

    plt.title(f"Format Comparison - {ds}")
    plt.xlabel("Format")
    plt.ylabel("Energy (Joules)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{ds}_comparison.png")
    plt.show()

    import seaborn as sns

pivot = df.pivot_table(
    index="Format",
    columns="Operation",
    values="Energy",
    aggfunc="mean"
)

plt.figure(figsize=(8,6))
sns.heatmap(pivot, annot=True, cmap="coolwarm")
plt.title("Energy Heatmap (Avg)")
plt.tight_layout()
plt.show()