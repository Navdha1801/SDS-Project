import geopandas as gpd
import os

file = "data/tanzania.gpkg"

# ✅ USE THIS LAYER
layer_name = "gis_osm_pois_a_free"

gdf = gpd.read_file(file, layer=layer_name)

print("Original size:", len(gdf))


print("Final size:", len(gdf))

os.makedirs("data", exist_ok=True)

gdf.to_file("data/sample.geojson", driver="GeoJSON")
gdf.to_file("data/sample.shp")
gdf.to_file("data/sample.gpkg", driver="GPKG")
gdf.to_parquet("data/sample.parquet")

print("✅ Converted to all formats")