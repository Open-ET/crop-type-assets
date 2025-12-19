# Crop Type Image Assets

This tool is used to generate the crop type image assets.  The crop type images are built in tiles following the MGRS tiling scheme used by the ET export tool.  Custom crop type and field extents will be read from field feature collections stored in Earth Engine.

## Approach

For each year, the following images are stacked and reduced using a firstNonNull reducer, so that pixels inside/touching a field get a single crop type, while those outside the field boundaries get the "best" or closest in time value.

- All fields with a crop type > 0 (and not equal to 176) for that year are rasterized
- All non-California MGRS tiles
  - Cropland Data Layer (CDL)
    - Years < 2008, use the annual crop remapped 2008 CDL image
    - Years 2008 to the last CDL year, use the CDL image directly
    - Years after the last CDL year, use the remapped last CDL image
- California MGRS tiles 10S, 10T, 11S
  - California DWR Crop Mapping data for California MGRS tiles 10S, 10T, 11S
      - 2014, 2016, 2018-2024 : use the CA Crop Mapping image directly
      - 2009-2013: use the remapped 2014.  
        - Note, the urban and managed wetland pixels are masked after remapping for these 5 years (check if this masking is needed for other years).
      - 2015 and 2017: use the remapped previous year (2014 and 2016)
      - 2025+: use the remapped 2024 year
  - Cropland Data Layer (CDL)
    - Almost the same as the approach for the non-California tiles above, except that we always use the annual crop remapped version of the CDL to fill any holes that might be in the CA Crop Mapping images

### Annual Crop Remapping

The annual crop remapping values are stored in the "cdl_annual_crop_remap_table.csv".

## Field Boundary Feature Collections

There should be a separate field boundary feature collection for each state you intend to include.  The location of the feature collections is currently hardcoded in the "crop_type_field_id" parameter in the script.  The naming of the state feature collections is currently hardcoded as the upper case of the state abbreviation (e.g. "CA" or "NV").

### Crop Type Fields

The crop type tool are currently assuming that the crop type fields are named "CROP_YYYY" where "YYYY" is the 4 digit year.  For example, the crop type values for 2017 would be stored in a field called "CROP_2017".

## Output Collection ID

The crop_type images are being written to image collections in the "projects/openet/assets/crop_type" folder.  The image collections are named based on the last year of CDL data used following the pattern "vYYYYa", where "YYYY" is the four digit year and the trailing letter is used to track versions.  Separate images are created for each MGRS tile and for each year.  The naming of the image ID is currently "<MGRS_ID>\_<YEAR_DATE>" (e.g. "10SFJ_20170101").

The most recent complete version of the crop type image assets is in the collection:

```
projects/openet/assets/crop_type/v2024b
```

## Running the Tools

The following command will start separate export tasks for each MGRS tile intersecting the study area specified in the parameter file.  This script will generate images for each MGRS tile and year and write these to a local folder.  The script will then upload them to a cloud storage bucket and start Earth Engine image upload calls for each image (using the earthengine command line tool).

```
python crop_type_asset_mgrs_collection.py --years 2018
```

### Command line arguments 

#### Help

The "-h"/"--help" argument can be passed to the script in order to see details about all of the possible command line arguments.

```
(python3) C:\Projects\openet-tools\crop_type>python crop_type_asset_mgrs_collection.py -h

usage: crop_type_asset_mgrs_collection.py [-h] [--tiles TILES [TILES ...]]
                                          [--years YEARS [YEARS ...]]
                                          [--key FILE] [--overwrite] [--debug]
                                         
Build crop type MGRS tiles from state feature collections

optional arguments:
  -h, --help            show this help message and exit
  --tiles TILES [TILES ...]
                        Comma/space separated list of MGRS tiles (default: )
  --years YEARS [YEARS ...]
                        Comma separated list and/or range of years (default: )
  --key FILE            JSON key file (default: None)
  --overwrite           Force overwrite of existing files (default: False)
  --debug               Debug level logging (default: 20)

```
#### Tiles

The "--tiles" argument can be used to limit which MGRS tiles are processed.

#### Years

The "--years" argument can be used to limit which years are processed.

#### Overwrite

The "--overwrite" argument must be passed to the script in order to overwrite any existing image assets.  If the overwrite flag is not set, any tiles that already have a crop_type image will be skipped.

#### Key

The "--key" argument can be used to initialize Earth Engine using a service account JSON key file.

#### Debug

The "--debug" argument can be passed to the script in order to activate Debug level logging.  This will print additional output to the command prompt and may help if the script is not running as expected.
