import os
import geopandas as gpd
from shapely.geometry import MultiPolygon
import shapely.ops as ops


cwd = os.getcwd()
# input_dir = os.path.join(cwd, "Input_SHP")
input_dir = os.path.join(cwd, "Input_SHP_All_Boundaries")
output_dir = os.path.join(cwd, "1_Sliver_Output")

# Helper function to calculate Polsby-Popper score
def calculate_polsby_popper(geometry):
    # Polsby-Popper score = 4 * π * Area / (Perimeter ^ 2)
    if geometry.is_empty or geometry.length == 0:
        return 0
    return 4 * 3.14159 * geometry.area / (geometry.length ** 2)

# check if output directory exists
if not os.path.exists(output_dir):
    # if the directory is not present, create it
    os.makedirs(output_dir)

for filename in os.listdir(input_dir):
    if filename.endswith(".shp"):  # Check if it's a shapefile
        filepath = os.path.join(input_dir, filename)
        print("\n\n\n")
        print(filename)
        # Read the shapefile
        try:
            gdf = gpd.read_file(filepath)
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            continue

        # Load original shapefile
        # input_file = "OR.shp"
        # gdf = gpd.read_file(os.path.join(cwd,"Input_SHP",input_file))

        # Reproject to Albers Equal Area (EPSG: 5070)
        gdf = gdf.to_crs(epsg=5070)

        # Convert multipart geometries to singlepart geometries
        gdf_exp = gdf.explode(index_parts=False)

        # Calculate area in acres (1 square meter = 0.000247105 acres)
        print("Calculating area...")
        gdf_exp['area_acres'] = gdf_exp.geometry.area * 0.000247105
        total_area  = round(gdf_exp['area_acres'].sum(), 2)
        print("Total area: "+ str(total_area) + " acres")


        # Calculate Polsby-Popper score
        gdf_exp['polsby_pop'] = gdf_exp.geometry.apply(calculate_polsby_popper)

        # Filter features with area less than 0.2 acres and Polsby-Popper score less than 0.5
        selected_gdf = gdf_exp[(gdf_exp['area_acres'] < 0.2) & (gdf_exp['polsby_pop'] < 0.5)]

        # Save selected features to a new shapefile
        selected_file = "sliver_features"+filename
        if os.path.exists(selected_file):
            # Delete the shapefile and its associated files
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                file_path = selected_file.replace('.shp', ext)
                if os.path.exists(file_path):
                    os.remove(file_path)
            print(f"Shapefile '{selected_file}' deleted successfully.")
        else:
            pass
            # print(f"Shapefile '{selected_file}' not found.")
        print("\nNumber of slivers: "+ str(len(selected_gdf)))
        total_area  = round(selected_gdf['area_acres'].sum(), 2)
        print("Total sliver area: "+ str(total_area) + " acres\n")
        selected_gdf.to_file(selected_file)

        # Perform erase operation (difference between original and selected features)
        # Ensure both GeoDataFrames are using the same CRS
        if not gdf.crs == selected_gdf.crs:
            selected_gdf = selected_gdf.to_crs(gdf.crs)

        # Perform the erase (difference) operation
        # print("Deleting slivers...")
        gdf_erased = gdf.overlay(selected_gdf, how='difference')
        gdf_erased = gdf_erased.to_crs(epsg=4326)

        # TODO: Delete points and line geometries if they exist
        gdf_erased = gdf_erased[gdf_erased.geometry.type.isin(['Polygon', 'MultiPolygon'])]


        # Save the result to a new shapefile
        output_file = "sliverrepair_" + filename
        if os.path.exists(output_file):
            # Delete the shapefile and its associated files
            for ext in ['.shp', '.shx', '.dbf', '.prj']:
                file_path = gdf_erased.replace('.shp', ext)
                if os.path.exists(file_path):
                    os.remove(file_path)
            print(f"Shapefile '{output_file}' deleted successfully.")
        else:
            pass
            # print(f"Shapefile '{output_file}' not found.")


        gdf_erased.to_file(os.path.join(output_dir, output_file))
        print("Erase operation completed and shapefile saved as:", output_file)

        # TODO: clear GDFs
        del gdf_erased
        del selected_gdf
        del gdf
        del gdf_exp
