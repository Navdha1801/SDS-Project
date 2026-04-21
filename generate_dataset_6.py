import geopandas as gpd
import os
import gzip
import shutil

# -----------------------------
# LOAD DATA
# -----------------------------
gdf = gpd.read_file("data/sample.geojson")

os.makedirs("compression_data", exist_ok=True)

# =====================================================
# PART A: PARQUET (TRUE COMPRESSION EXPERIMENT)
# =====================================================

print("\nSaving Parquet variants...")

# 1. Uncompressed
gdf.to_parquet(
    "compression_data/parquet_uncompressed.parquet",
    compression=None
)

# 2. Snappy
gdf.to_parquet(
    "compression_data/parquet_snappy.parquet",
    compression="snappy"
)

# 3. ZSTD
gdf.to_parquet(
    "compression_data/parquet_zstd.parquet",
    compression="zstd"
)

# =====================================================
# PART B: GEOJSON (GZIP COMPRESSION)
# =====================================================

print("Saving GeoJSON variants...")

# Normal
geojson_path = "compression_data/geojson_normal.geojson"
gdf.to_file(geojson_path, driver="GeoJSON")

# GZIP compressed
with open(geojson_path, "rb") as f_in:
    with gzip.open("compression_data/geojson_gzip.geojson.gz", "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

# =====================================================
# PART C: GPKG (SIMPLIFIED VERSION)
# =====================================================

print("Saving GeoPackage variants...")

# Normal
gdf.to_file("compression_data/normal_gpkg.gpkg", driver="GPKG")

# Simplified geometry
gdf_simple = gdf.copy()
gdf_simple["geometry"] = gdf_simple.geometry.simplify(0.0001)

gdf_simple.to_file("compression_data/simplified_gpkg.gpkg", driver="GPKG")