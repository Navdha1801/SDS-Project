import geopandas as gpd
import os

gdf = gpd.read_file("data/sample.geojson")

#gdf = gdf.set_crs(epsg=4326)
if gdf.crs is None:
    gdf = gdf.set_crs(epsg=4326)

os.makedirs("index_data", exist_ok=True)

# -----------------------------
# WITHOUT INDEX (all formats)
# -----------------------------
print("Saving WITHOUT index...")

gdf.to_file("index_data/sample_noindex.geojson", driver="GeoJSON")
gdf.to_file("index_data/sample_noindex.shp")
gdf.to_file("index_data/sample_noindex.gpkg", driver="GPKG")
gdf.to_parquet("index_data/sample_noindex.parquet")

# -----------------------------
# WITH INDEX (same data)
# -----------------------------
print("Saving WITH index (logical)...")

gdf.to_file("index_data/sample_index.gpkg", driver="GPKG")

print("✅ Index datasets ready (index handled during query phase)")