import os
import geopandas as gpd
from shapely.geometry.polygon import orient


# Specify the input and output paths
cwd = os.getcwd()
input_dir = os.path.join(cwd, "1_Sliver_Output")
output_dir = os.path.join(cwd, "2_Righthand_Output")
# shapefile = "OR.shp"

# check if output directory exists
if not os.path.exists(output_dir):
    # if the directory is not present, create it
    os.makedirs(output_dir)

for filename in os.listdir(input_dir):
    if filename.endswith(".shp"):  # Check if it's a shapefile
        filepath = os.path.join(input_dir, filename)
        print(filename)
        # Read the shapefile
        try:
            gdf = gpd.read_file(filepath)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue


        # Ensure each geometry follows the right-hand rule
        def fix_orientation(geom):
            if geom.geom_type == 'Polygon':
                return orient(geom, sign=1.0)  # Ensure counterclockwise orientation
            elif geom.geom_type == 'MultiPolygon':
                # Apply right-hand rule to each part of the MultiPolygon
                return type(geom)([orient(part, sign=1.0) for part in geom.geoms])
            else:
                return geom  # Leave other geometries (like points or lines) unchanged

        # Apply the orientation fix to all geometries
        gdf['geometry'] = gdf['geometry'].apply(fix_orientation)

        # Save the updated GeoDataFrame back to a new shapefile
        # output_filename = filename.replace("sliv_", "")
        output_filename = filename.replace("sliverrepair_", "")
        gdf.to_file(os.path.join(output_dir, output_filename))




