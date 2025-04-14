import argparse
import logging
import os
# import pprint

import ee
from google.cloud import storage
import pandas as pd

import openet.core.utils as utils

STORAGE_CLIENT = storage.Client(project='openet')

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def main(states, years=[], overwrite_flag=False, gee_key_file=None, project_id=None):
    """Export field crop type geojson by state

    Parameters
    ----------
    states : list
    years : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
    project_id : str, optional
        Google cloud project ID to use for GEE authentication.
        This will be checked after the gee_key_file and before the user Initialize.
        The default is None.

    Returns
    -------

    """
    logging.info('\nExport field crop type stats files by state')

    # Min/max year range to process
    # This should be 1997 to present year (or present year - 1) unless earlier CDL images are developed
    year_min = 1997
    year_max = 2024

    cdl_coll_id = 'USDA/NASS/CDL'

    # Years where CDL has full CONUS coverage
    # Don't change min year unless additional CONUS CDL images are ingested
    cdl_year_min = 2008
    cdl_year_max = 2024

    ca_coll_id = 'projects/openet/assets/crop_type/california'
    nlcd_coll_id = 'projects/sat-io/open-datasets/USGS/ANNUAL_NLCD/LANDCOVER'

    field_folder_id = f'projects/openet/assets/features/fields/temp'

    bucket_name = 'openet_geodatabase'
    bucket_folder = 'temp_croptype_20250414'

    output_format = 'CSV'

    if states == ['ALL']:
        # 'AL' is not included since there is not an Alabama field shapefile
        states = [
            'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'ID', 'IL', 'IN', 'IA',
            'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT',
            'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA',
            'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY',
        ]
    else:
        states = sorted(list(set(y.strip() for x in states for y in x.split(',') if y.strip())))
    logging.info(f'States: {", ".join(states)}')

    if not years:
        years = range(year_min, year_max+1)
    else:
        years = {
            int(year) for year_str in years
            for year in utils.str_ranges_2_list(year_str)
            if ((year <= year_max) and (year >= year_min))
        }
    years = sorted(list(years), reverse=True)
    logging.info(f'Years:  {", ".join(map(str, years))}')

    # All states are available for 2008 through present
    # These lists may not be complete for the eastern states
    # Not including CA 2007, WA 2006, ID 2005
    cdl_year_states = {year: states for year in range(cdl_year_min, cdl_year_max+1)}
    cdl_year_states[2007] = ['AR', 'IA', 'ID', 'IL', 'IN', 'KS', 'LA', 'MI',
                             'MN', 'MO', 'MS', 'MT', 'ND', 'NE', 'OH', 'OK', 'OR',
                             'SD', 'WA', 'WI']
    cdl_year_states[2006] = ['AR', 'IA', 'IL', 'IN', 'KS', 'LA', 'MN', 'MO',
                             'MS', 'ND', 'NE', 'OH', 'OK', 'SD', 'WI']
    cdl_year_states[2005] = ['AR', 'IA', 'IL', 'IN', 'MO', 'MS', 'ND', 'NE', 'WI']
    cdl_year_states[2004] = ['AR', 'FL', 'IA', 'IL', 'IN', 'MO', 'MS', 'ND', 'NE', 'WI']
    cdl_year_states[2003] = ['AR', 'IA', 'IL', 'IN', 'MO', 'MS', 'ND', 'NE', 'WI']
    cdl_year_states[2002] = ['AR', 'IA', 'IL', 'IN', 'MO', 'MS', 'ND', 'NE',
                             'NC', 'VA', 'WV', 'MD', 'DE', 'PA', 'NJ', 'NY', 'CT', 'RI']
    cdl_year_states[2001] = ['AR', 'IA', 'IL', 'IN', 'MO', 'MS', 'ND', 'NE']
    cdl_year_states[2000] = ['AR', 'IA', 'IL', 'IN', 'MS', 'ND']
    cdl_year_states[1999] = ['AR', 'IL', 'MS', 'ND']
    cdl_year_states[1998] = ['ND']
    cdl_year_states[1997] = ['ND']

    # Identify the years that are available for each state
    # Apply the user defined year filtering here also
    cdl_state_years = {state: [] for state in states}
    for year in years:
        if year not in cdl_year_states.keys():
            continue
        for state in cdl_year_states[year]:
            try:
                cdl_state_years[state].append(year)
            except KeyError:
                pass

    # Load the CDL annual crop remap
    # TODO: Get the script path instead (in case it is different than the cwd)
    remap_path = os.path.join(os.path.dirname(os.getcwd()), 'cdl_annual_crop_remap_table.csv')
    remap_df = pd.read_csv(remap_path, comment='#').sort_values(by='IN')
    cdl_annual_remap = dict(zip(remap_df.IN, remap_df.OUT))
    # Set all unassigned values to remap to themselves
    for cdl_code in set(range(1, 256)) - set(cdl_annual_remap.keys()):
        cdl_annual_remap[cdl_code] = cdl_code
    cdl_remap_in, cdl_remap_out = map(list, zip(*cdl_annual_remap.items()))

    # Initialize Earth Engine
    if gee_key_file:
        logging.info(f'\nInitializing GEE using user key file: {gee_key_file}')
        try:
            ee.Initialize(ee.ServiceAccountCredentials('_', key_file=gee_key_file))
        except ee.ee_exception.EEException:
            logging.warning('Unable to initialize GEE using user key file')
            return False
    elif project_id is not None:
        logging.info(f'\nInitializing Earth Engine using project credentials'
                     f'\n  Project ID: {project_id}')
        try:
            ee.Initialize(project=project_id)
        except Exception as e:
            logging.warning(f'\nUnable to initialize GEE using project ID\n  {e}')
            return False
    else:
        logging.info('\nInitializing Earth Engine using default credentials')
        ee.Initialize()

    # Get current running tasks
    tasks = utils.get_ee_tasks()
    if logging.getLogger().getEffectiveLevel() == logging.DEBUG:
        logging.debug(f'  Tasks: {len(tasks)}')
        # input('ENTER')

    logging.info('\nGetting bucket file list')
    bucket = STORAGE_CLIENT.get_bucket(bucket_name)
    bucket_files = sorted([
        x.name.replace(bucket_folder + '/', '')
        for x in bucket.list_blobs(prefix=bucket_folder + '/')
        if x.name.replace(bucket_folder + '/', '')
    ])


    # Process CDL stats first
    for state in states:
        # California is processed separately below
        if state == 'CA':
            continue

        logging.info(f'\n{state} CDL')

        field_coll_id = f'{field_folder_id}/{state}'
        field_coll = ee.FeatureCollection(field_coll_id)

        for year in years:
            export_id = f'{state}_cdl_{year}'.lower()

            # Only process states that are present in the CDL image
            # Missing years will be filled with the "fill_missing_crop_types.py" tool
            if state not in cdl_state_years.keys() or year not in cdl_state_years[state]:
                continue
            logging.info(f'{export_id}')

            if overwrite_flag:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, cancelling')
                    ee.data.cancelTask(tasks[export_id]['id'])
                if export_id in bucket_files:
                    logging.info('  File already exists in bucket, overwriting')
                    # TODO: Uncomment if export doesn't overwrite
                    # img_blob = bucket.blob(f'{bucket_folder}/{export_id}.tif')
                    # img_blob.delete()
            else:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, skipping')
                    continue
                if f'{export_id}.csv' in bucket_files:
                    logging.info('  File already exists in bucket, skipping')
                    continue

            # For post-2022, use the annual crop remapped 2022 image
            # For pre-2008 years, if state specific images are not available,
            #   use the annual crop remapped 2008 images for all years
            # The 2005 and 2007 CDL images have slightly different naming
            #   because they are split into two images (a & b)
            # Otherwise, use state specific CDL image directly
            if year > cdl_year_max:
                # Remapping directly to the crop type image since the remap
                #   table was modified to map all missing values to them self
                # The .where() would be needed if the remap was incomplete
                #     .where(remap_img, 47)
                cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
                cdl_img = (
                    ee.Image(cdl_img_id)
                    .select(['cropland'], [f'CROP_{year}'])
                    .remap(cdl_remap_in, cdl_remap_out)
                )
                crop_source = f'{cdl_img_id} - remapped annual crops'
            elif (year < cdl_year_min) and (year not in cdl_state_years[state]):
                # NOTE: This condition can currently never happen because
                #   of year filtering at beginning of for loop
                cdl_img_id = f'{cdl_coll_id}/{cdl_year_min}'
                cdl_img = (
                    ee.Image(cdl_img_id)
                    .select(['cropland'], [f'CROP_{year}'])
                    .remap(cdl_remap_in, cdl_remap_out)
                )
                crop_source = f'{cdl_img_id} - remapped annual crops'
            elif year == 2005:
                if state == 'ID':
                    # # Condition is not possible if year/state is not in cdl_year_states
                    # #   but leaving check just in case
                    # cdl_img_id = f'{cdl_coll_id}/2005'
                    raise Exception('ID 2005 CDL image should not be used')
                elif state == 'MS':
                    cdl_img_id = f'{cdl_coll_id}/2005b'
                else:
                    cdl_img_id = f'{cdl_coll_id}/2005a'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'], [f'CROP_{year}'])
                crop_source = f'{cdl_img_id}'
            # elif year == 2006 and state == 'WA':
            #     # # Condition is not possible if year/state is not in cdl_year_states
            #     # #   but leaving check just in case
            #     raise Exception('WA 2006 CDL image should not be used')
            elif year == 2007:
                if state == 'CA':
                    # # Condition is not possible if year/state is not in cdl_year_states
                    # #   but leaving check just in case
                    # cdl_img_id = f'{cdl_coll_id}/2007b'
                    raise Exception('CA 2007b CDL image should not be used')
                else:
                    cdl_img_id = f'{cdl_coll_id}/2007a'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'], [f'CROP_{year}'])
                crop_source = f'{cdl_img_id}'
            else:
                cdl_img_id = f'{cdl_coll_id}/{year}'
                cdl_img = ee.Image(cdl_img_id).select(['cropland'], [f'CROP_{year}'])
                crop_source = f'{cdl_img_id}'

            # Mask any cloud/nodata pixels (mostly in pre-2008 years)
            cdl_img = cdl_img.updateMask(cdl_img.neq(81))

            # Select the NLCD year
            # Use the first/last available year if outside the available range
            nlcd_coll = ee.ImageCollection(nlcd_coll_id)
            nlcd_year = (
                ee.Number(year)
                .max(ee.Date(nlcd_coll.aggregate_min('system:time_start')).get('year'))
                .min(ee.Date(nlcd_coll.aggregate_max('system:time_start')).get('year'))
            )
            nlcd_date = ee.Date.fromYMD(nlcd_year, 1, 1)
            nlcd_img = (
                ee.ImageCollection(nlcd_coll_id)
                .filterDate(nlcd_date, nlcd_date.advance(1, 'year')).first()
                .select([0], ['landcover'])
            )

            # Change any CDL 176 and NLCD 81/82 pixels to 37
            cdl_img = cdl_img.where(
                cdl_img.eq(176).And(nlcd_img.eq(81).Or(nlcd_img.eq(82))), 37
                # cdl_img.eq(176).And(nlcd_img.neq(71)), 37
            )

            # Compute the mode
            crop_type_coll = cdl_img.reduceRegions(
                reducer=ee.Reducer.mode().unweighted(),
                collection=field_coll,
                crs=cdl_img.projection(),
                crsTransform=ee.List(ee.Dictionary(
                    ee.Algorithms.Describe(cdl_img.projection())).get('transform')),
            )

            # Cleanup the output collection before exporting
            def set_properties(ftr):
                return ee.Feature(None, {
                    'OPENET_ID': ftr.get('OPENET_ID'),
                    f'CROP_{year}': ftr.get('mode'),
                    f'CSRC_{year}': crop_source,
                })
            crop_type_coll = ee.FeatureCollection(crop_type_coll.map(set_properties))

            # logging.debug('  Building export task')
            task = ee.batch.Export.table.toCloudStorage(
                collection=crop_type_coll,
                description=export_id,
                bucket=bucket_name,
                fileNamePrefix=f'{bucket_folder}/{export_id}',
                fileFormat=output_format,
            )

            logging.info('  Starting export task')
            utils.ee_task_start(task)


    # First compute California Crop Mapping zonal stats without merging with CDL
    if 'CA' in states:
        state = 'CA'
        logging.info(f'\nCA Crop Mapping Datasets')

        field_coll_id = f'{field_folder_id}/{state}'
        field_coll = ee.FeatureCollection(field_coll_id)

        for year in years:
            # NOTE: This could be restructured to not compute all the years
            #   since many of them will be identical (i.e. 2008-2013)

            if year < 2009:
                logging.debug('Not using California Crop Mapping before 2009 - skipping')
                continue

            # Computing zonal stats on the EPSG:6414 raster
            # To switch to the UTM zone images,
            #   update the California Crop Mapping image/export ID
            export_id = f'{state}_landiq_{year}'.lower()
            logging.info(f'{export_id}')

            if overwrite_flag:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, cancelling')
                    ee.data.cancelTask(tasks[export_id]['id'])
                if export_id in bucket_files:
                    logging.info('  File already exists in bucket, overwriting')
                    # TODO: Uncomment if export doesn't overwrite
                    # img_blob = bucket.blob(f'{bucket_folder}/{export_id}.tif')
                    # img_blob.delete()
            else:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, skipping')
                    continue
                if f'{export_id}.csv' in bucket_files:
                    logging.info('  File already exists in bucket, skipping')
                    continue

            # TODO: Check what should be the first year to start using California Crop Mapping
            #   Starting before 2009 makes switching to CDL 2008 a little tricky

            # Select the California image
            if year in [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]:
                # Use the California image directly for years when it is present
                ca_img_id = f'{ca_coll_id}/{year}'
                ca_img = ee.Image(ca_img_id)
            elif year > 2023:
                ca_img_id = f'{ca_coll_id}/2023'
                ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
            elif year in [2015, 2017]:
                ca_img_id = f'{ca_coll_id}/{year-1}'
                ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
            elif year < 2009:
                logging.debug('Not using California Crop Mapping before 2009 - skipping')
                continue
            elif year in [2009, 2010, 2011, 2012, 2013]:
                # Use a 2014 remapped annual crop image for all pre-2014 years
                # Remove the urban and managed wetland polygons for pre2014 years
                ca_img_id = f'{ca_coll_id}/2014'
                ca_img = (
                    ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
                    .updateMask(ee.Image(ca_img_id).neq(82))
                    .updateMask(ee.Image(ca_img_id).neq(87))
                )
            else:
                raise Exception(f'unexpected California Crop Mapping year: {year}')

            if year in [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]:
                crop_src = f'{ca_img_id}'
            else:
                crop_src = f'{ca_img_id} - remapped annual crops'

            # Add the mask and unmasked image to get the pixel counts
            mask_img = ca_img.gt(0)
            unmask_img = mask_img.unmask()
            crop_type_img = ca_img.addBands([mask_img, unmask_img])

            reducer = (
                ee.Reducer.mode().unweighted()
                .combine(ee.Reducer.sum().unweighted())
                .combine(ee.Reducer.count().unweighted())
            )

            crop_type_coll = crop_type_img.reduceRegions(
                reducer=reducer,
                collection=field_coll,
                crs=crop_type_img.projection(),
                crsTransform=ee.List(ee.Dictionary(ee.Algorithms.Describe(
                    crop_type_img.projection())).get('transform')),
            )

            # Cleanup the output collection
            def set_properties(ftr):
                return ee.Feature(None, {
                    'OPENET_ID': ftr.get('OPENET_ID'),
                    f'CROP_{year}': ftr.getNumber('mode'),
                    f'CSRC_{year}': crop_src,
                    f'PIXEL_COUNT': ftr.getNumber('sum'),
                    f'PIXEL_TOTAL': ftr.getNumber('count'),
                })
            crop_type_coll = crop_type_coll.map(set_properties)

            # logging.debug('  Building export task')
            task = ee.batch.Export.table.toCloudStorage(
                collection=ee.FeatureCollection(crop_type_coll),
                description=export_id,
                bucket=bucket_name,
                fileNamePrefix=f'{bucket_folder}/{export_id}',
                fileFormat=output_format,
            )
            logging.info('  Starting export task')
            utils.ee_task_start(task)


    # Then compute zonal stats with CA Crop Mapping / CDL composite
    if 'CA' in states:
        state = 'CA'
        logging.info(f'\nCA Crop Mapping / CDL Composite')

        field_coll_id = f'{field_folder_id}/{state}'
        field_coll = ee.FeatureCollection(field_coll_id)

        for year in years:
            if year < cdl_year_min:
                continue

            export_id = f'{state}_composite_{year}'.lower()
            logging.info(f'{export_id}')

            if overwrite_flag:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, cancelling')
                    ee.data.cancelTask(tasks[export_id]['id'])
                if export_id in bucket_files:
                    logging.info('  File already exists in bucket, overwriting')
                    # TODO: Uncomment if export doesn't overwrite
                    # img_blob = bucket.blob(f'{bucket_folder}/{export_id}.tif')
                    # img_blob.delete()
            else:
                if export_id in tasks.keys():
                    logging.info('  Task already submitted, skipping')
                    continue
                if f'{export_id}.csv' in bucket_files:
                    logging.info('  File already exists in bucket, skipping')
                    continue

            # Select the California Crop Mapping image
            # The pre2009 filtering is handled below when the mosaic is made
            # if year < 2009:
            #     logging.debug('Not using California Crop Mapping before 2009 - skipping')
            #     continue
            if year in [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]:
                # Use the California Crop Mapping directly for years when it is present
                ca_img_id = f'{ca_coll_id}/{year}'
                ca_img = ee.Image(ca_img_id)
            elif year > 2023:
                ca_img_id = f'{ca_coll_id}/2023'
                ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
            elif year in [2015, 2017]:
                ca_img_id = f'{ca_coll_id}/{year-1}'
                ca_img = ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
            elif year < 2014:
                # Use a 2014 remapped annual crop image for all pre-2014 years
                # Remove the urban and managed wetland polygons for pre2014 years
                ca_img_id = f'{ca_coll_id}/2014'
                ca_img = (
                    ee.Image(ca_img_id).remap(cdl_remap_in, cdl_remap_out)
                    .updateMask(ee.Image(ca_img_id).neq(82))
                    .updateMask(ee.Image(ca_img_id).neq(87))
                )

            # Select the CDL image to use
            # For California, always use the annual remapped CDL
            # Use a 2008 remapped annual crop image for all pre-2008 years
            # Use a 2023 remapped annual crop image for all post-2023 years
            cdl_img_id = f'{cdl_coll_id}/{min(max(year, cdl_year_min), cdl_year_max)}'
            cdl_img = ee.Image(cdl_img_id).select(['cropland'], ['cdl'])

            # Mask any cloud/nodata pixels (mostly in pre-2008 years)
            # Probably not needed for California but including to be consistent
            cdl_img = cdl_img.updateMask(cdl_img.neq(81))

            # Remap was modified to map all missing values to them self
            # The .where() would be needed if the remap was incomplete
            #     .where(remap_img, 47)
            cdl_img = cdl_img.remap(cdl_remap_in, cdl_remap_out)

            # Select the NLCD year
            # Use the first/last available year if outside the available range
            nlcd_coll = ee.ImageCollection(nlcd_coll_id)
            nlcd_year = (
                ee.Number(year)
                .max(ee.Date(nlcd_coll.aggregate_min('system:time_start')).get('year'))
                .min(ee.Date(nlcd_coll.aggregate_max('system:time_start')).get('year'))
            )
            nlcd_date = ee.Date.fromYMD(nlcd_year, 1, 1)
            nlcd_img = (
                ee.ImageCollection(nlcd_coll_id)
                .filterDate(nlcd_date, nlcd_date.advance(1, 'year')).first()
                .select([0], ['landcover'])
            )

            # Change any CDL 176 and NLCD 81/82 pixels to 37
            cdl_img = cdl_img.where(
                cdl_img.eq(176).And(nlcd_img.eq(81).Or(nlcd_img.eq(82))), 37
                # cdl_img.eq(176).And(nlcd_img.neq(71)), 37
            )

            # Mosaic the image with California Crop Mapping images on top
            # For pre2008 images don't use Crop Mapping images
            if year < 2009:
                crop_type_img = cdl_img.reduce(ee.Reducer.firstNonNull())
                crop_source = f'{cdl_img_id} - remapped annual crops'
            else:
                crop_type_img = ee.Image([ca_img, cdl_img]).reduce(ee.Reducer.firstNonNull())
                crop_source = f'CA{ca_img_id.split("/")[-1]} ' \
                              f'CDL{cdl_img_id.split("/")[-1]} composite' \
                              f' - remapped annual crops'

            # Compute zonal stats on the mosaiced images using the Crop Mapping
            #   image crs and transform
            crop_type_coll = crop_type_img\
                .reduceRegions(
                    reducer=ee.Reducer.mode().unweighted(),
                    collection=field_coll,
                    crs=ca_img.projection(),
                    crsTransform=ee.List(ee.Dictionary(ee.Algorithms.Describe(
                        ca_img.projection())).get('transform')),
                )

            # Cleanup the output collection
            def set_properties(ftr):
                return ee.Feature(None, {
                    'OPENET_ID': ftr.get('OPENET_ID'),
                    f'CROP_{year}': ftr.getNumber('mode'),
                    f'CSRC_{year}': crop_source,
                })
            crop_type_coll = crop_type_coll.map(set_properties)
            # crop_type_coll = crop_type_coll.select(['.*'], None, False)

            # logging.debug('  Building export task')
            task = ee.batch.Export.table.toCloudStorage(
                collection=ee.FeatureCollection(crop_type_coll),
                description=export_id,
                bucket=bucket_name,
                fileNamePrefix=f'{bucket_folder}/{export_id}',
                fileFormat=output_format,
            )

            logging.info('  Starting export task')
            utils.ee_task_start(task)


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export field crop type stats files by state',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', default=['ALL'], nargs='+',
        help='Comma/space separated list of states')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma/space separated years and/or ranges of years')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='GEE service account key file')
    parser.add_argument(
        '--project', default=None,
        help='Google cloud project ID to use for GEE authentication')
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

    main(
        states=args.states,
        years=args.years,
        overwrite_flag=args.overwrite,
        gee_key_file=args.key,
        project_id=args.project,
    )
