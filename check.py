import geopandas as gpd
import fiona

file = "data/maldives.gpkg"

layers = fiona.listlayers(file)
print("Available layers:", layers)

# pick first layer
gdf = gpd.read_file(file, layer=layers[0])

print(gdf.head())
print("Total rows:", len(gdf))