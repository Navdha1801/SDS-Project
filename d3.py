import matplotlib.pyplot as plt

operations = ["SELECT", "JOIN"]

# Your values
no_index_energy = [0.0001905, 0.0002016]
with_index_energy = [0.0002226, 0.0001916]

x = range(len(operations))

plt.figure()

plt.plot(x, no_index_energy, marker='o', label="No Index")
plt.plot(x, with_index_energy, marker='o', label="With Index")

plt.xticks(x, operations)
plt.xlabel("Operation")
plt.ylabel("Energy (Joules)")
plt.title("Index vs No Index Energy Comparison")
plt.legend()

plt.tight_layout()
plt.show()