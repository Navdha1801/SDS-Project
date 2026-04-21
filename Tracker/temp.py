import geopandas as gpd
import numpy as np
from shapely.geometry import Point
import time, os

# Generate and save test files in each format
np.random.seed(42)
n = 200_000
lons = np.random.uniform(72.5, 73.5, n)
lats = np.random.uniform(3.5, 7.5, n)
gdf = gpd.GeoDataFrame(
    {"name": [f"POI_{i}" for i in range(n)], "value": np.random.randint(0, 100, n)},
    geometry=[Point(x, y) for x, y in zip(lons, lats)],
    crs="EPSG:4326"
)

formats = {
    "geojson":    ("data/test.geojson",   {}),
    "shapefile":  ("data/test.shp",       {}),
    "gpkg":       ("data/test.gpkg",      {"driver": "GPKG"}),
    "flatgeobuf": ("data/test.fgb",       {"driver": "FlatGeobuf"}),
    "parquet":    ("data/test.parquet",   None),  # None = use to_parquet
}

os.makedirs("data", exist_ok=True)

# Write all formats first
for fmt, (path, kwargs) in formats.items():
    if kwargs is None:
        gdf.to_parquet(path)
    else:
        gdf.to_file(path, **kwargs)
    size_mb = os.path.getsize(path) / 1024**2
    print(f"Written {fmt:<12} → {size_mb:.1f} MB")

print()

# Time reads
for fmt, (path, _) in formats.items():
    t = time.time()
    if path.endswith(".parquet"):
        loaded = gpd.read_parquet(path)
    else:
        loaded = gpd.read_file(path)
    elapsed = time.time() - t
    print(f"READ {fmt:<12} | {elapsed:.3f}s | {len(loaded):,} rows")