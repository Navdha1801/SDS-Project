import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# =====================================================
# CREATE OUTPUT FOLDER
# =====================================================
os.makedirs("d2_plots", exist_ok=True)

# =====================================================
# LOAD DATA
# =====================================================
df = pd.read_csv("d2_plots/geometry_experiments.csv")

# =====================================================
# CLEAN DATA
# =====================================================
df = df.dropna()

# =====================================================
# CONVERT JOULES → MICROJOULES
# =====================================================
df["energy_uJ"] = df["mean_energy"] * 1_000_000

# =====================================================
# ORDER GEOMETRY TYPES
# =====================================================
geom_order = [
    "points",
    "lines",
    "simple_polygons",
    "complex_polygons"
]

df["geometry"] = pd.Categorical(
    df["geometry"],
    categories=geom_order,
    ordered=True
)

# =====================================================
# PLOT 1: ENERGY vs GEOMETRY
# =====================================================
operations = df["operation"].unique()

for op in operations:

    plt.figure(figsize=(8,6))

    subset = df[df["operation"] == op]

    for fmt in subset["format"].unique():

        fmt_data = subset[
            subset["format"] == fmt
        ].sort_values("geometry")

        x = fmt_data["geometry"].astype(str).tolist()
        y = fmt_data["energy_uJ"].tolist()

        plt.plot(
            x,
            y,
            marker='o',
            linewidth=2,
            label=fmt
        )

    plt.title(f"Energy vs Geometry ({op})")
    plt.xlabel("Geometry Type")
    plt.ylabel("Mean Energy (µJ)")
    plt.legend()
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d2_plots/{op}_geometry_energy_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 2: TIME vs GEOMETRY
# =====================================================
for op in operations:

    plt.figure(figsize=(8,6))

    subset = df[df["operation"] == op]

    for fmt in subset["format"].unique():

        fmt_data = subset[
            subset["format"] == fmt
        ].sort_values("geometry")

        x = fmt_data["geometry"].astype(str).tolist()
        y = fmt_data["mean_time"].tolist()

        plt.plot(
            x,
            y,
            marker='o',
            linewidth=2,
            label=fmt
        )

    plt.title(f"Time vs Geometry ({op})")
    plt.xlabel("Geometry Type")
    plt.ylabel("Mean Time (seconds)")
    plt.legend()
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d2_plots/{op}_geometry_time.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 3: FORMAT COMPARISON
# =====================================================
geometries = df["geometry"].unique()

for geom in geometries:

    plt.figure(figsize=(8,6))

    subset = df[df["geometry"] == geom]

    for op in subset["operation"].unique():

        op_data = subset[
            subset["operation"] == op
        ]

        x = op_data["format"].tolist()
        y = op_data["energy_uJ"].tolist()

        plt.plot(
            x,
            y,
            marker='o',
            linewidth=2,
            label=op
        )

    plt.title(f"Format Comparison ({geom})")
    plt.xlabel("Format")
    plt.ylabel("Mean Energy (µJ)")
    plt.xticks(rotation=45)
    plt.legend()
    plt.tight_layout()

    # SAVE
    plt.savefig(
        f"d2_plots/{geom}_format_comparison_microjoules.png",
        dpi=300
    )

    plt.close()

# =====================================================
# PLOT 4: ENERGY HEATMAP
# =====================================================
pivot = df.pivot_table(
    index="geometry",
    columns="format",
    values="energy_uJ",
    aggfunc="mean"
)

plt.figure(figsize=(8,6))

sns.heatmap(
    pivot,
    annot=True,
    cmap="coolwarm",
    fmt=".2f"
)

plt.title("Energy Heatmap (Geometry vs Format)")
plt.tight_layout()

# SAVE
plt.savefig(
    "d2_plots/geometry_heatmap_microjoules.png",
    dpi=300
)

plt.close()

print("\n✅ All Dimension 2 plots saved in d2_plots/")