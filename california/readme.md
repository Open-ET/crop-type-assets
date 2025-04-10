# California Crop Type Assets

Manually download the crop mapping shapefiles from the California Statewide Crop Mapping website to the "california/sources/" folder.
https://data.cnra.ca.gov/dataset/statewide-crop-mapping

Then run the "ca_shp_to_image_asset.py" tool to convert the shapefiles to GEE image assets.

### Remap tables
There is now a single remap table for the period of 2016-2023 based on DWR documentation. The remap of 2014 will use a year-specific table.

