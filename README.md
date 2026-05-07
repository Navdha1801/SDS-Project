# SpatialJoules

Energy Profiling of Spatial Data Formats

## Overview

SpatialJoules measures the energy consumed by common spatial operations (SELECT, INSERT, UPDATE, DELETE, JOIN) across different geospatial file formats. It is directly inspired by [DBJoules](https://github.com/rishalab/DBJoules), which benchmarks energy across relational and NoSQL databases — this project applies the same methodology to spatial formats.

**Formats benchmarked:** GeoJSON, Shapefile (SHP), GeoPackage (GPKG), GeoParquet

**Experiments:**

| ID | Experiment | Variable |
|----|------------|----------|
| D1 | Format vs Operation | Dataset size (Small / Medium / Large) |
| D2 | Geometry Complexity | Points, Lines, Simple Polygons, Complex Polygons |
| D3 | Spatial Indexing | With vs Without spatial index |
| D6 | Compression Codec | Parquet (uncompressed / Snappy / Zstd), GeoJSON (normal / gzip), GPKG (normal / simplified) |

---

## Repository Structure

```
SDS-Project/
├── Tracker/
│   ├── main.py          # Tracker class, @track decorator, all run_*_operation functions
│   └── utils.py         # CPU/RAM helpers, config I/O
├── main_app.py          # Experiment runner (choose D1/D2/D3/D6) + Flask web app
├── d1.py                # Plot generation for D1
├── d2.py                # Plot generation for D2
├── d3.py                # Plot generation for D3
├── d6.py                # Plot generation for D6
├── d1_plots/            # D1 results and charts
├── d2_plots/            # D2 results and charts
├── d3_plots/            # D3 results and charts
├── d6_plots/            # D6 results and charts
├── generate_dataset_1.py  # Converts OSM Tanzania data → all formats (D1)
├── generate_dataset_2.py  # Converts OSM Maldives data → geometry datasets (D2)
├── generate_dataset_3.py  # Builds indexed / non-indexed GPKG files (D3)
├── generate_dataset_6.py  # Builds compressed variants (D6)
├── compression_data/    # Pre-built files for D6
├── index_data/          # Pre-built files for D3
├── docs/
│   └── dependencies.txt
└── Experiments/         # Inherited DBJoules CSV outputs and query scripts
```

---

## How It Works

### Energy Measurement (`Tracker/main.py`)

The `Tracker` class wraps a code block between `.start()` and `.stop()` calls. At `stop()` it reads CPU% and RAM% via `psutil` and computes:

```
cpu_energy  = cpu_percent  × 1e-5   (joules proxy)
ram_energy  = ram_percent  × 1e-6   (joules proxy)
total_energy = cpu_energy + ram_energy
```

The `@track` decorator applies this automatically, returning `(duration, energy, result)`.

### Experiment Runner (`run_experiment`)

Each experiment runs the target operation **30 times**. The first 10 and last 10 runs are discarded (warm-up / cool-down stabilization). Mean and standard deviation are computed over the remaining 10 runs.

---

## Datasets

| Experiment | Source File | Geometry Types |
|------------|-------------|----------------|
| D1 | `data/tanzania.gpkg` (OSM Tanzania) | Polygons of interest |
| D2 | `data/maldives.gpkg` (OSM Maldives) | Points, Lines, Simple Polygons, Complex Polygons |
| D3 | `index_data/sample_[no]index.gpkg` | Polygons |
| D6 | `compression_data/` | Mixed |

Dataset sizes used in D1: **Small** (Maldives), **Medium** (Bosnia), **Large** (Tanzania).

---

## Running Experiments

### 1. Generate datasets

```bash
python generate_dataset_1.py   # D1 — format datasets
python generate_dataset_2.py   # D2 — geometry datasets
python generate_dataset_3.py   # D3 — index datasets
python generate_dataset_6.py   # D6 — compression datasets
```

### 2. Run an experiment

```bash
python main_app.py
```

You will be prompted to pick one:

```
1 → Format vs Operation (Dataset Size)
2 → Geometry Complexity Experiment
3 → Spatial Index (With vs Without Index)
6 → Compression Codec Experiment
```

Results are appended/written to the corresponding `d*_plots/*.csv` file.

### 3. Generate plots

```bash
python d1.py   # Reads d1_plots/format_experiment.csv  → saves PNGs to d1_plots/
python d2.py   # Reads d2_plots/geometry_experiments.csv
python d3.py   # Reads d3_plots/index_experiment.csv
python d6.py   # Reads d6_plots/compression_experiment.csv
```

---

## Web App (inherited from DBJoules)

`main_app.py` also contains a Flask web application for the original DBJoules workflow — measuring energy for SQL/NoSQL queries across MySQL, PostgreSQL, MongoDB, and Couchbase.

```bash
python main_app.py   # starts Flask on http://127.0.0.1:5000
```

This part requires local installations of MySQL, PostgreSQL, MongoDB, and/or Couchbase.

---

## Dependencies

Install with `pip install <name>`:

```
geopandas
pandas
numpy
matplotlib
seaborn
psutil
flask
werkzeug
psycopg2
pymongo
mysql-connector-python
couchbase
fiona
shapely
pyarrow
```

See `docs/dependencies.txt` for the full list.

---

## Acknowledgements

This project extends the methodology of [DBJoules](https://github.com/rishalab/DBJoules) (Towards Energy Efficient Databases) to the domain of spatial data formats. Geospatial data sourced from [OpenStreetMap](https://www.openstreetmap.org/) via Geofabrik.
