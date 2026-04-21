import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# -----------------------------
# LOAD DATA
# -----------------------------
df = pd.read_csv("d2_plots/geometry_experiment.csv")

# Clean
df = df.dropna()

# -----------------------------
# ORDER GEOMETRY
# -----------------------------
geom_order = ["points", "lines", "simple_polygons", "complex_polygons"]
df["geometry"] = pd.Categorical(df["geometry"], categories=geom_order, ordered=True)

# -----------------------------
# PLOT 1: ENERGY vs GEOMETRY
# -----------------------------
operations = df["operation"].unique()

for op in operations:
    plt.figure()

    subset = df[df["operation"] == op]

    for fmt in subset["format"].unique():
        fmt_data = subset[subset["format"] == fmt].sort_values("geometry")

        x = fmt_data["geometry"].astype(str).tolist()
        y = fmt_data["mean_energy"].tolist()

        plt.plot(x, y, marker='o', label=fmt)

    plt.title(f"Energy vs Geometry ({op})")
    plt.xlabel("Geometry Type")
    plt.ylabel("Mean Energy (Joules)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{op}_geometry_energy.png")
    plt.show()

# -----------------------------
# PLOT 2: TIME vs GEOMETRY
# -----------------------------
for op in operations:
    plt.figure()

    subset = df[df["operation"] == op]

    for fmt in subset["format"].unique():
        fmt_data = subset[subset["format"] == fmt].sort_values("geometry")

        x = fmt_data["geometry"].astype(str).tolist()
        y = fmt_data["mean_time"].tolist()

        plt.plot(x, y, marker='o', label=fmt)

    plt.title(f"Time vs Geometry ({op})")
    plt.xlabel("Geometry Type")
    plt.ylabel("Mean Time (seconds)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{op}_geometry_time.png")
    plt.show()

# -----------------------------
# PLOT 3: FORMAT COMPARISON (PER GEOMETRY)
# -----------------------------
geometries = df["geometry"].unique()

for geom in geometries:
    plt.figure()

    subset = df[df["geometry"] == geom]

    for op in subset["operation"].unique():
        op_data = subset[subset["operation"] == op]

        x = op_data["format"].tolist()
        y = op_data["mean_energy"].tolist()

        plt.plot(x, y, marker='o', label=op)

    plt.title(f"Format Comparison ({geom})")
    plt.xlabel("Format")
    plt.ylabel("Mean Energy (Joules)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"{geom}_format_comparison.png")
    plt.show()

# -----------------------------
# PLOT 4: HEATMAP (VERY IMPORTANT)
# -----------------------------
pivot = df.pivot_table(
    index="geometry",
    columns="format",
    values="mean_energy",
    aggfunc="mean"
)

plt.figure(figsize=(8,6))
sns.heatmap(pivot, annot=True, cmap="coolwarm")
plt.title("Energy Heatmap (Geometry vs Format)")
plt.tight_layout()
plt.show()