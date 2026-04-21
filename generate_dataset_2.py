import geopandas as gpd
import fiona
import os

file = "data/maldives.gpkg"

layers = fiona.listlayers(file)
print("Available layers:", layers)

# -----------------------------
# SELECT REQUIRED LAYERS
# -----------------------------
layer_map = {
    "points": "gis_osm_pois_free",
    "lines": "gis_osm_roads_free",
    "simple_polygons": "gis_osm_buildings_a_free",
    "complex_polygons": "gis_osm_landuse_a_free"
}

os.makedirs("geom_data", exist_ok=True)

for key, layer_name in layer_map.items():
    print(f"\nProcessing {key} → {layer_name}")

    gdf = gpd.read_file(file, layer=layer_name)

    print(f"{key} size:", len(gdf))


    gdf.to_file(f"geom_data/{key}.geojson", driver="GeoJSON")
    gdf.to_parquet(f"geom_data/{key}.parquet")
    gdf.to_file(f"geom_data/{key}.gpkg", driver="GPKG")
    gdf.to_file(f"geom_data/{key}.shp")

print("\n✅ All geometry datasets prepared!")