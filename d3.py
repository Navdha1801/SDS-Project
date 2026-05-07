import os
import pandas as pd
import matplotlib.pyplot as plt

# =====================================================
# CREATE OUTPUT FOLDER
# =====================================================
os.makedirs("d3_plots", exist_ok=True)

# =====================================================
# LOAD CSV
# =====================================================
df = pd.read_csv("d3_plots/index_experiment.csv")

# =====================================================
# CLEAN DATA
# =====================================================
df = df.dropna()

# =====================================================
# CONVERT JOULES → MICROJOULES
# =====================================================
df["energy_uJ"] = df["mean_energy"] * 1_000_000

# =====================================================
# UNIQUE OPERATIONS
# =====================================================
operations = df["operation"].unique().tolist()

# =====================================================
# SPLIT CASES
# =====================================================
no_index = df[df["case"] == "NO_INDEX"]
with_index = df[df["case"] == "WITH_INDEX"]

# =====================================================
# SORT TO ENSURE SAME ORDER
# =====================================================
no_index = no_index.sort_values("operation")
with_index = with_index.sort_values("operation")

# =====================================================
# EXTRACT VALUES
# =====================================================
no_index_energy = no_index["energy_uJ"].tolist()
with_index_energy = with_index["energy_uJ"].tolist()

# =====================================================
# X AXIS
# =====================================================
x = range(len(operations))

# =====================================================
# PLOT
# =====================================================
plt.figure(figsize=(8,6))

plt.plot(
    x,
    no_index_energy,
    marker='o',
    linewidth=2,
    label="No Index"
)

plt.plot(
    x,
    with_index_energy,
    marker='o',
    linewidth=2,
    label="With Index"
)

plt.xticks(x, operations)

plt.xlabel("Operation")
plt.ylabel("Energy (µJ)")
plt.title("Index vs No Index Energy Comparison")

plt.legend()
plt.tight_layout()

# =====================================================
# SAVE
# =====================================================
plt.savefig(
    "d3_plots/index_vs_noindex_microjoules.png",
    dpi=300
)

plt.close()

print("\n✅ Plot saved successfully in d3_plots/")