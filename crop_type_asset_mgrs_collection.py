#--------------------------------
# Name:         crop_type_asset_mgrs_collection.py
# Purpose:      Build crop type MGRS tiles from the crop feature collections
#--------------------------------

import argparse
from datetime import datetime, timezone
import logging
import os
import pprint

import ee
import pandas as pd

import openet.core
import openet.core.utils as utils

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

TOOL_NAME = 'crop_type_asset_mgrs_collection'
# TOOL_NAME = os.path.basename(__file__)
TOOL_VERSION = '0.3.1'


def main(
        years=None,
        mgrs_tiles=None,
        utm_zones=None,
        overwrite_flag=False,
        delay=0,
        gee_key_file=None
):
    """Build and ingest crop type MGRS tiles from a feature collection

    Parameters
    ----------
    years : list, optional
    mgrs_tiles : list, optional
    utm_zones : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    delay : float, optional
        Delay time between each export task (the default is 0).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).

    Returns
    -------
    None

    """
    logging.info('\nBuild crop type MGRS tiles from the crop feature collections')

    # Hardcoded parameters
    project_id = 'projects/openet/assets'

    export_coll_id = f'{project_id}/crop_type/v2024a'
    # export_band_name = 'crop_type'

    crop_type_folder_id = f'{project_id}/features/fields/temp'

    # Using ERA5-Land MGRS tiles to avoid clipping outside CONUS
    mgrs_ftr_coll_id = f'{project_id}/mgrs/global/era5land/zones'
    mgrs_mask_coll_id = f'{project_id}/mgrs/global/era5land/zone_mask'
    # mgrs_ftr_coll_id = f'{project_id}/mgrs/conus/gridmet/zones'
    # mgrs_mask_coll_id = f'{project_id}/mgrs/conus/gridmet/zone_mask'

    cdl_coll_id = 'USDA/NASS/CDL'

    # California specific crop type images built from Califoria crop mapping data
    ca_coll_id = f'{project_id}/crop_type/california'

    # The states collection is being used to select the field collections (by name)
    states_coll_id = 'TIGER/2018/States'
    states_name_property = 'STUSPS'

    supported_mgrs_tiles = [
        '10S', '10T', '10U', '11R', '11S', '11T', '11U', '12R', '12S', '12T', '12U',
        '13R', '13S', '13T', '13U', '14R', '14S', '14T', '14U', '15R', '15S', '15T', '15U',
        '16R', '16S', '16T', '16U', '17R', '17S', '17T', '17U', '18S', '18T', '18U',
        '19T', '19U'
    ]
    # supported_mgrs_tiles = [
    #     '10S', '10T', '10U', '11S', '11T', '11U', '12R', '12S', '12T', '12U',
    #     '13R', '13S', '13T', '13U', '14R', '14S', '14T', '14U', '15R', '15S', '15T', '15U',
    #     '16R', '16S', '16T', '16U', '17R', '17S', '17T', '17U', '18S', '18T', '19T',
    # ]
    mgrs_skip_list = []

    annual_remap_path = os.path.join(os.getcwd(), 'cdl_annual_crop_remap_table.csv')

    year_min = 1985
    year_max = 2024

    # Parse user inputs
    if not years:
        years = list(range(year_min, year_max+1))
    else:
        years = set(
            int(year) for year_str in years
            for year in utils.str_ranges_2_list(year_str)
            if ((year <= year_max) and (year >= year_min))
        )
        years = sorted(list(years))
    logging.info(f'Years: {", ".join(map(str, years))}')

    if mgrs_tiles:
        mgrs_tiles = sorted([y.strip() for x in mgrs_tiles for y in x.split(',')])
        # mgrs_tiles = sorted([x.strip() for x in mgrs_tiles.split(',')])
        logging.info(f'User MGRS Tiles: {", ".join(mgrs_tiles)}')

    # Limit the MGRS tile list to the default tile list (if set)
    # Otherwise just use the supported tile list
    if mgrs_tiles and supported_mgrs_tiles:
        mgrs_tiles = [mgrs for mgrs in mgrs_tiles if mgrs in supported_mgrs_tiles]
    else:
        mgrs_tiles = supported_mgrs_tiles[:]

    if utm_zones:
        utm_zones = sorted([y.strip() for x in utm_zones for y in x.split(',')])
        mgrs_tiles = [mgrs for mgrs in mgrs_tiles if mgrs[:2] in utm_zones]
    logging.info(f'MGRS Tiles: {", ".join(mgrs_tiles)}')


    # Load the CDL annual crop remap
    # TODO: Get the script path instead (in case it is different than the cwd)
    remap_df = pd.read_csv(annual_remap_path, comment='#').sort_values(by='IN')
    cdl_annual_remap = dict(zip(remap_df.IN, remap_df.OUT))
    # Set all unassigned values to remap to themselves
    for cdl_code in set(range(1, 256)) - set(cdl_annual_remap.keys()):
        cdl_annual_remap[cdl_code] = cdl_code
    cdl_remap_in, cdl_remap_out = map(list, zip(*cdl_annual_remap.items()))


    # Add a CDL remapped version of the NALCMS 2020 image last
    # Using 47 for the ag class for now
    # Mapping the polar classes to 0 for now (11, 12, 13 lichen-moss)
    # TODO: Doublecheck remap values
    nalcms_img_id = 'USGS/NLCD_RELEASES/2020_REL/NALCMS'
    nalcms_cdl_remap = [
        [1, 142],   # Temperate or sub-polar needleleaf forest
        [2, 142],   # Sub-polar taiga needleleaf forest
        [3, 142],   # Tropical or sub-tropical broadleaf evergreen forest
        [4, 141],   # Tropical or sub-tropical broadleaf deciduous forest
        [5, 141],   # Temperate or sub-polar broadleaf deciduous forest
        [6, 143],   # Mixed forest
        [7, 152],   # Tropical or sub-tropical shrubland
        [8, 152],   # Temperate or sub-polar shrubland
        [9, 176],   # Tropical or sub-tropical grassland
        [10, 176],  # Temperate or sub-polar grassland
        [11, 0],    # Sub-polar or polar shrubland-lichen-moss
        [12, 0],    # Sub-polar or polar grassland-lichen-moss
        [13, 0],    # Sub-polar or polar barren-lichen-moss
        [14, 195],  # Wetland
        [15, 47],   # Cropland
        [16, 131],  # Barren lands
        [17, 123],  # Urban and built-up
        [18, 111],  # Water
        [19, 112],  # Snow and ice
    ]
    nalcms_cdl_remap = list(zip(*nalcms_cdl_remap))


    logging.info('\nInitializing Earth Engine')
    if gee_key_file:
        logging.info(f'  Using service account key file: {gee_key_file}')
        # The "EE_ACCOUNT" parameter is not used if the key file is valid
        ee.Initialize(ee.ServiceAccountCredentials('', key_file=gee_key_file))
    else:
        ee.Initialize()


    # Get current running tasks
    tasks = utils.get_ee_tasks()
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        logging.debug(f'  Tasks: {len(tasks)}')
        input('ENTER')


    # Build the export collection if it doesn't exist
    if not ee.data.getInfo(export_coll_id.rsplit('/', 1)[0]):
        logging.info('\nFolder does not exist and will be built'
                      '\n  {}'.format(export_coll_id.rsplit('/', 1)[0]))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'FOLDER'}, export_coll_id.rsplit('/', 1)[0])
    if not ee.data.getInfo(export_coll_id):
        logging.info('\nExport collection does not exist and will be built'
                     '\n  {}'.format(export_coll_id))
        input('Press ENTER to continue')
        ee.data.createAsset({'type': 'IMAGE_COLLECTION'}, export_coll_id)


    # Get list of existing images/files
    # CGM - Note that "projects/earthengine-legacy/assets/" is not in the ID
    logging.debug('\nGetting GEE asset list')
    asset_list = utils.get_ee_assets(export_coll_id)
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        pprint.pprint(asset_list[:10])


    # Get list of MGRS tiles that intersect the study area
    # Intentionally using the MGRS collection as the study area collection
    #   since the MGRS tile list has been filtered to the supported tiles
    logging.debug('\nBuilding export list')
    export_list = mgrs_export_tiles(
        study_area_coll_id=mgrs_ftr_coll_id,
        mgrs_coll_id=mgrs_ftr_coll_id,
        mgrs_tiles=mgrs_tiles,
        # mgrs_skip_list=mgrs_skip_list,
        # utm_zones=utm_zones,
    )
    if not export_list:
        logging.error('\nEmpty export list, exiting')
        return False
    # export_list = sorted(export_list, reverse=reverse_flag, key=lambda i: i['index'])


    # Get a list of the available state field feature collections
    crop_type_states = [
        asset['id'].split('/')[-1]
        for asset in ee.data.listAssets({'parent': crop_type_folder_id})['assets']
        if asset['type'] == 'TABLE'
    ]
    logging.info(f'\nStates with field feature collections:\n  {", ".join(crop_type_states)}')


    # Get the last available CDL year
    cdl_year_min = 2008
    cdl_year_max = int(utils.get_info(
        ee.ImageCollection(cdl_coll_id)
        .limit(1, 'system:time_start', False).first()
        .get('system:index')
    ))
    logging.info(f'\nLast available CDL year: {cdl_year_max}')


    # Process each tile separately
    logging.info('\nImage Exports')
    for export_n, export_info in enumerate(export_list):
        mgrs_tile = export_info['index'].upper()
        logging.info(f'MGRS Tile: {mgrs_tile} ({export_n + 1}/{len(export_list)})')
        logging.debug(f'  Shape:      {export_info["shape_str"]}')
        logging.debug(f'  Transform:  {export_info["geo_str"]}')
        logging.debug(f'  Extent:     {export_info["extent"]}')
        logging.debug(f'  MaxPixels:  {export_info["maxpixels"]}')

        mgrs_geom = ee.Geometry.Rectangle(
            export_info['extent'], proj=export_info['crs'], geodesic=False
        )

        mgrs_mask_id = f'{mgrs_mask_coll_id}/{mgrs_tile.lower()}'
        mgrs_mask_img = ee.Image(mgrs_mask_id)
        # logging.debug(f'{mgrs_mask_id}')
        # pprint.pprint(mgrs_mask_img.getInfo())

        # Get a list of states that could intersect the MGRS tile
        # Use this state list to select the field collections
        state_coll = ee.FeatureCollection(states_coll_id).filterBounds(mgrs_geom)
        mgrs_states = state_coll.aggregate_array(states_name_property).getInfo()
        logging.debug(f'  States intersecting the MGRS tile/zone: '
                      f'{", ".join(sorted(mgrs_states))}')

        logging.info('  Building crop type feature collection')
        field_coll = ee.FeatureCollection([])
        # CGM - I think this should be an "and", and the check in the loop isn't needed
        field_states = sorted(list(set(mgrs_states) & set(crop_type_states)))
        # field_states = sorted(list(set(mgrs_states) | set(crop_type_states)))
        logging.info(f'    States: {", ".join(sorted(field_states))}')
        for state in field_states:
            # if state not in crop_type_states:
            #     continue
            # logging.debug(f'    {state}')
            crop_type_coll_id = f'{crop_type_folder_id}/{state.upper()}'
            crop_type_coll = ee.FeatureCollection(crop_type_coll_id).filterBounds(mgrs_geom)
            field_coll = field_coll.merge(crop_type_coll)

        # CGM - There may be tiles without fields, especially for the eastern
        #   states, but we still want to build an image.  For now, just pause
        #   and force the user to acknowledge that the field count is 0.
        # Need to check if the rasterize will work if there are no fields
        # field_count = field_coll.size().getInfo()
        # logging.info(f'  Features: {field_count}')
        # if field_count <= 0:
        #     input('ENTER')
        #     # logging.info('  No intersecting features - skipping')
        #     # continue

        for year in years:
            image_id = f'{mgrs_tile}_{year}0101'
            asset_id = f'{export_coll_id}/{image_id}'
            asset_short_id = asset_id.replace('projects/earthengine-legacy/assets/', '')
            export_id = f'crop_type_{image_id}'
            crop_type_field = f'CROP_{year}'
            logging.info(f'{asset_id}')
            logging.debug(f'  {asset_short_id}')
            logging.debug(f'  {export_id}')

            if overwrite_flag:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, cancelling')
                    ee.data.cancelTask(tasks[export_id])
                # This is intentionally not an "elif" so that a task can be
                # cancelled and an existing image/file/asset can be removed
                if asset_id in asset_list or asset_short_id in asset_list:
                    logging.info('  Asset already exists, removing')
                    ee.data.deleteAsset(asset_id)
            else:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, exiting')
                    continue
                elif asset_id in asset_list or asset_short_id in asset_list:
                    logging.info('  Asset already exists, skipping')
                    continue

            properties = {
                'system:time_start': ee.Date.fromYMD(year, 1, 1).millis(),
                'core_version': openet.core.__version__,
                'crop_type_folder': crop_type_folder_id,
                'crop_type_states': ','.join(mgrs_states),
                'date_ingested': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                'mgrs_tile': mgrs_tile,
                'tool_name': TOOL_NAME,
                'tool_version': TOOL_VERSION,
            }

            # # DEADBEEF
            # print(
            #     field_coll.filter(ee.Filter.gt(crop_type_field, 0))
            #     .filter(ee.Filter.rangeContains(crop_type_field, 80.5, 195.5).Not())
            #     .size().getInfo()
            # )
            # input('ENTER')

            # Start with the MGRS mask set to 0
            output_img = mgrs_mask_img.updateMask(0)

            # Rasterize the fields
            # 176 fields will not be burned in (for now)
            # Long term they should probably be reassigned in the field collections
            # Added the uint8 since image was coming back as a double
            # Filtering on +/-0.5 values to handle floating point numbers in the crop types
            field_img = (
                field_coll.filter(ee.Filter.gt(crop_type_field, 0))
                .filter(ee.Filter.rangeContains(crop_type_field, 80.5, 195.5).Not())
                .reduceToImage([crop_type_field], ee.Reducer.first())
                .round().uint8()
            )
            output_img = output_img.addBands(field_img.rename(['fields']))
            properties['field_states'] = ','.join(field_states)


            # Mosaic the California Crop Mapping image for the UTM zone before any CDL images
            if mgrs_tile in ['10S', '10T', '11S']:
                if year in [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]:
                    # Using the UTM zone projected version of the California image
                    ca_img_id = f'{ca_coll_id}/{year}_utm{mgrs_tile[:2]}'
                    # ca_img_id = f'{ca_coll_id}/{year}'
                    ca_img = ee.Image(ca_img_id)
                elif year > 2023:
                    ca_img_id = f'{ca_coll_id}/2023_utm{mgrs_tile[:2]}'
                    # ca_img_id = f'{ca_coll_id}/2021'
                    ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
                elif year in [2015, 2017]:
                    ca_img_id = f'{ca_coll_id}/{year-1}_utm{mgrs_tile[:2]}'
                    # ca_img_id = f'{ca_coll_id}/{year - 1}'
                    ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
                elif year in [2009, 2010, 2011, 2012, 2013]:
                    # Use a 2014 remapped annual crop image for pre-2014 years
                    # Remove the urban and managed wetland polygons
                    ca_img_id = f'{ca_coll_id}/2014_utm{mgrs_tile[:2]}'
                    # ca_img_id = f'{ca_coll_id}/2014'
                    ca_img = (
                        ee.Image(ca_img_id)
                        .remap(cdl_remap_in, cdl_remap_out)
                        .updateMask(ee.Image(ca_img_id).neq(82))
                        .updateMask(ee.Image(ca_img_id).neq(87))
                    )
                elif year < 2009:
                    # Don't include before 2009
                    ca_img = None
                else:
                    logging.warning(f'unexpected year: {year}')
                    input('ENTER')

                if ca_img:
                    output_img = output_img.addBands(ca_img.rename(['landiq']))
                    properties['custom_ca_img_id'] = ca_img_id


            # For California, always use the annual remapped CDL
            # This is different than the generic CDL section below where the
            #   annual remap is only used for pre-2008 and current years
            # Use a 2008 remapped annual crop image for all pre-2008 years
            # For all years after the last available CDL year,
            #   use an annual crop remapped version of the last CDL year
            if mgrs_tile in ['10S', '10T', '11S']:
                if year < cdl_year_min:
                    ca_cdl_img_id = f'{cdl_coll_id}/{cdl_year_min}'
                elif year > cdl_year_max:
                    ca_cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
                else:
                    ca_cdl_img_id = f'{cdl_coll_id}/{year}'

                ca_cdl_img = (
                    ee.Image(ca_cdl_img_id).select(['cropland'])
                    .remap(cdl_remap_in, cdl_remap_out)
                )
                # The clip could be changed to a mask using the CIMIS mask?
                ca_geom = (
                    ee.FeatureCollection('TIGER/2018/States')
                    .filterMetadata('STUSPS', 'equals', 'CA').first().geometry()
                )
                ca_cdl_img = ca_cdl_img.clip(ca_geom)
                output_img = output_img.addBands(ca_cdl_img.rename(['cdl_ca_img']))
                properties['cdl_ca_img_id'] = ca_cdl_img_id


            # For any years after the last available CDL year
            #   use the annual crop remapped version of the last year image
            # For pre-2008 years, use the annual crop remapped 2008 images
            if year > cdl_year_max:
                cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
                cdl_img = (
                    ee.Image(cdl_img_id).select(['cropland'])
                    .remap(cdl_remap_in, cdl_remap_out)
                )
            elif year < cdl_year_min:
                cdl_img_id = f'{cdl_coll_id}/{cdl_year_min}'
                cdl_img = (
                    ee.Image(cdl_img_id).select(['cropland'])
                    .remap(cdl_remap_in, cdl_remap_out)
                )
            else:
                cdl_img_id = f'{cdl_coll_id}/{year}'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'])
            output_img = output_img.addBands(cdl_img.rename(['cdl_conus_img']))
            properties['cdl_img_id'] = cdl_img_id


            # Use the remapped NALCMS (North America) image as the fallback image
            nalcms_img = (
                ee.Image(nalcms_img_id)
                .remap(nalcms_cdl_remap[0], nalcms_cdl_remap[1])
                .rename(['nalcms_img'])
            )
            output_img = output_img.addBands(nalcms_img)
            properties['nalcms_img_id'] = nalcms_img_id


            # Build the output image from the stack
            output_img = (
                output_img
                .reduce(ee.Reducer.firstNonNull())
                .updateMask(mgrs_mask_img)
                .rename(['cropland'])
                .set(properties)
            )

            # Build export tasks
            task = ee.batch.Export.image.toAsset(
                output_img,
                description=export_id,
                assetId=asset_id,
                dimensions=export_info['shape_str'],
                crs=export_info['crs'],
                crsTransform=export_info['geo_str'],
                maxPixels=export_info['maxpixels']*2,
                pyramidingPolicy={'cropland': 'mode'},
            )

            logging.info('  Starting export task')
            utils.ee_task_start(task)

            # Pause before starting the next export task
            utils.delay_task(delay)

            logging.debug('')


def mgrs_export_tiles(
        study_area_coll_id,
        mgrs_coll_id,
        study_area_property=None,
        study_area_features=[],
        mgrs_tiles=[],
        mgrs_skip_list=[],
        utm_zones=[],
        mgrs_property='mgrs',
        utm_property='utm',
        cell_size=30
        ):
    """Select MGRS tiles and metadata that intersect the study area geometry

    Parameters
    ----------
    study_area_coll_id : str
        Study area feature collection asset ID.
    mgrs_coll_id : str
        MGRS feature collection asset ID.
    study_area_property : str, optional
        Property name to use for inList() filter call of study area collection.
        Filter will only be applied if both 'study_area_property' and
        'study_area_features' parameters are both set.
    study_area_features : list, optional
        List of study area feature property values to filter on.
    mgrs_tiles : list, optional
        User defined MGRS tile subset.
    mgrs_skip_list : list, optional
        User defined list MGRS tiles to skip.
    utm_zones : list, optional
        User defined UTM zone subset.
    mgrs_property : str, optional
        MGRS property in the MGRS feature collection (the default is 'mgrs').
    utm_property : str, optional
        UTM zone property in the MGRS feature collection (the default is 'wrs2').
    cell_size : float, optional
        Cell size for transform and shape calculation (the default is 30).

    Returns
    ------
    list of dicts: export information

    """
    # Build and filter the study area feature collection
    logging.debug('Building study area collection')
    logging.debug(f'  {study_area_coll_id}')
    study_area_coll = ee.FeatureCollection(study_area_coll_id)
    if ((study_area_property == 'STUSPS') and
            ('CONUS' in [x.upper() for x in study_area_features])):
        # Exclude AK, HI, AS, GU, PR, MP, VI
        # We don't really need all the states since the MGRS tiles are large
        #   but leaving as is for now
        study_area_features = [
            'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA',
            'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME',
            'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ',
            'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD',
            'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY',
        ]
    study_area_features = sorted(list(set(study_area_features)))

    if study_area_property and study_area_features:
        logging.debug('  Filtering study area collection')
        logging.debug(f'  Property: {study_area_property}')
        logging.debug(f'  Features: {",".join(study_area_features)}')
        study_area_coll = study_area_coll.filter(
            ee.Filter.inList(study_area_property, study_area_features)
        )

    logging.info('\nBuilding MGRS tile list')
    tiles_coll = ee.FeatureCollection(mgrs_coll_id).filterBounds(study_area_coll.geometry())

    # Filter collection by user defined lists
    if utm_zones:
        logging.debug(f'  Filter User UTM Zones:    {utm_zones}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList(utm_property, utm_zones))
    if mgrs_skip_list:
        logging.debug(f'  Filter MGRS skip list:    {mgrs_skip_list}')
        tiles_coll = tiles_coll.filter(ee.Filter.inList('mgrs', mgrs_skip_list).Not())
    if mgrs_tiles:
        logging.debug(f'  Filter MGRS tiles/zones:  {mgrs_tiles}')
        # Allow MGRS tiles to be subsets of the full tile code
        #   i.e. mgrs_tiles = 10TE, 10TF
        mgrs_filters = [
            ee.Filter.stringStartsWith(mgrs_property, mgrs_id.upper())
            for mgrs_id in mgrs_tiles
        ]
        tiles_coll = tiles_coll.filter(ee.call('Filter.or', mgrs_filters))

    # Drop the MGRS tile geometry to simplify the getInfo call
    def drop_geometry(ftr):
        return ee.Feature(None).copyProperties(ftr)

    logging.debug('  Requesting tile/zone info')
    tiles_info = utils.get_info(tiles_coll.map(drop_geometry))

    tiles_list = []
    for tile_ftr in tiles_info['features']:
        mgrs_id = tile_ftr['properties']['mgrs'].upper()
        tile_extent = [
            int(tile_ftr['properties']['xmin']), int(tile_ftr['properties']['ymin']),
            int(tile_ftr['properties']['xmax']), int(tile_ftr['properties']['ymax'])
        ]
        tile_geo = [cell_size, 0, tile_extent[0], 0, -cell_size, tile_extent[3]]
        tile_shape = [
            int((tile_extent[2] - tile_extent[0]) / cell_size),
            int((tile_extent[3] - tile_extent[1]) / cell_size)
        ]
        tiles_list.append({
            'crs': 'EPSG:{:d}'.format(int(tile_ftr['properties']['epsg'])),
            'extent': tile_extent,
            # 'geo': tile_geo,
            'geo_str': '[' + ','.join(map(str, tile_geo)) + ']',
            'index': mgrs_id,
            'maxpixels': tile_shape[0] * tile_shape[1],
            # 'shape': tile_shape,
            'shape_str': '{0}x{1}'.format(*tile_shape),
            'utm': int(mgrs_id[:2]),
        })

    export_list = [tile for tile in sorted(tiles_list, key=lambda k: k['index'])]

    return export_list


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Build crop type MGRS tiles from state feature collections',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--mgrs', default='', nargs='+',
        help='Comma/space separated list of MGRS tiles')
    parser.add_argument(
        '--utm', default='', nargs='+',
        help='Comma/space separated list of UTM zones')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma separated list and/or range of years')
    parser.add_argument(
        '--delay', default=0, type=float,
        help='Delay (in seconds) between each export tasks')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='JSON key file')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')
    logging.getLogger('googleapiclient').setLevel(logging.ERROR)

    main(
        years=args.years,
        mgrs_tiles=args.mgrs,
        utm_zones=args.utm,
        overwrite_flag=args.overwrite,
        delay=args.delay,
        gee_key_file=args.key,
    )
