# Crop Type Field Tools

These tools are used to populate the initial crop type values for the field shapefiles.

## Preprocess

The preprococess tool will first ingest the bucket shapefiles into a temp folder in Earth Engine.  The shapefiles are ingested into Earth Engine so that the zonal stats call in the next tool can operate on Earth Engine feature collections and images instead of trying to do the calculation locally.

The tool will also download and unzip the shapefiles and add the crop field fields to each shapefile if they are not present.  The default crop type value for each field will be set to 0 if the field is added.

```
python preprocess_shapefiles.py --states AZ
```

## Zonal Stats

The field crop type is computed using a majority reducer so that the dominant CDL pixel value in the field is used as the field crop type

## Export/Update field crop type by MGRS

These tools will use the Earth Engine feature collection to compute the crop type.  

The export tool will make a separate zonal stats call for each MGRS zone (i.e. "11S") that intersects the state.  The zonal stats are saved as geojson files to a Google Cloud Storage bucket.

The update tool will then collect the geojson files and update the crop type values in the shapefile based on these values.

```
python export_field_crop_type_by_state.py --states ALL
```

```
python update_field_crop_type_by_state.py --states ALL
```

### Crop Type Remappings

The separate classes for annual crops are all being remapped to CDL crop type 47.

#### California

The crop type remappings are different for California since we are currently using the California DWR Crop Mapping data (https://data.cnra.ca.gov/dataset/statewide-crop-mapping) and not CDL for all years after 2008.

* 2008 - Use remapped annual CDL instead of CA Crop Mapping for all years prior to 2009
* 2009-2013 - Use remapped annual 2014 CA Crop Mapping
* 2014 - Use 2014 CA Crop Mapping directly
* 2015 - Use remapped annual 2014 CA Crop Mapping
* 2016 - Use 2016 CA Crop Mapping directly
* 2017 - Use remapped annual 2016 CA Crop Mapping
* 2018-2023 - Use CA Crop Mapping directly
* 2024 - Use remapped annual 2023 CA C

#### Other States

* pre-2008 - Use remapped annual 2008 CDL for years prior to 2008
* 2008-2023 - Use remapped annual CDL
* post-2024 - Use remapped annual 2024 CDL for all years after 2024

## Replace bad CDL crop type values in New Mexico and Colorado

```
python replace_bad_crop_types.py
```

## Fill missing crop type values

```
python fill_missing_crop_types.py --states ALL
```

## Postprocess

```
python postprocess_shapefiles.py --states ALL
```
