import argparse
import logging
import os
# import pprint

import ee
from google.cloud import storage
import pandas as pd

import openet.core.utils as utils

PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def main(states, years=[], overwrite_flag=False, gee_key_file=None):
    """Export field crop type geojson by state

    Parameters
    ----------
    states : list
    years : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).

    Returns
    -------

    """
    logging.info('\nExport field crop type stats files by state')

    cdl_coll_id = 'USDA/NASS/CDL'
    ca_coll_id = 'projects/openet/assets/crop_type/california'

    project_id = 'projects/openet/assets'

    field_folder_id = f'{project_id}/features/fields/temp'
    # field_folder_id = f'{project_id}/features/fields/2024-02-01'

    bucket_name = 'openet_geodatabase'
    bucket_folder = 'temp_croptype_20250409'

    output_format = 'CSV'
    # output_format = 'GeoJSON'

    if states == ['ALL']:
        # 'AL' is not included since there is not an Alabama field shapefile
        states = [
            'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'ID', 'IL', 'IN', 'IA',
            'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT',
            'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA',
            'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY',
        ]
    else:
        states = sorted(list(set(
            y.strip() for x in states for y in x.split(',') if y.strip()
        )))
    logging.info(f'States: {", ".join(states)}')

    # This CDL start year is for the full CONUS images, but CDL does exist for
    #   some states back to 1997 (see cdl_year_states dictionary below)
    cdl_year_min = 1997
    cdl_year_max = 2024

    # Min/max year range to process
    year_min = 2008
    year_max = 2024
    # year_max = datetime.datetime.today().year

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
    # Including 2023 since it will be available soon (as of Dec 2023)
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


    # Setting the in between years explicitly
    # Selecting the previous NLCD year when difference is equal
    # Will use first/last available year for years outside provided range
    # TODO: Double check this decision
    nlcd_img_ids = {
        2021: 'USGS/NLCD_RELEASES/2021_REL/NLCD/2021',
        2020: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2019',
        2019: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2019',
        2018: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2019',
        2017: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2016',
        2016: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2016',
        2015: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2016',
        2014: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2013',
        2013: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2013',
        2012: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2011',
        2011: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2011',
        2010: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2011',
        2009: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2008',
        2008: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2008',
        2007: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2006',
        2006: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2006',
        2005: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2004',
        2004: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2004',
        2003: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2004',
        2002: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2001',
        2001: 'USGS/NLCD_RELEASES/2019_REL/NLCD/2001',
    }


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
            elif year < cdl_year_min and year not in cdl_state_years[state]:
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
            nlcd_year = min(year, max(nlcd_img_ids.keys()))
            nlcd_year = max(nlcd_year, min(nlcd_img_ids.keys()))
            nlcd_img_id = nlcd_img_ids[nlcd_year]
            nlcd_img = ee.Image(nlcd_img_id).select('landcover')

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


    # First compute California LandIQ zonal stats without merging with CDL
    if 'CA' in states:
        state = 'CA'
        logging.info(f'\nCA Statewide Crop Mapping Datasets')

        field_coll_id = f'{field_folder_id}/{state}'
        field_coll = ee.FeatureCollection(field_coll_id)

        # DEADBEEF
        # Compute the zonal stats separately for each UTM zone
        # This may not be necessary and the results may be the same as
        #   as pulling from the unprojected (EPSG:6414) image asset
        # Would need to change the LandIQ image ID
        # Would need to add a filter to field_coll
        #   .filter(ee.Filter.stringStartsWith('MGRS_TILE', f'{utm_zone}'))

        for year in years:
            # NOTE: This could be restructured to not compute all the years
            #   since many of them will be identical (i.e. 2008-2013)

            if year < 2009:
                logging.debug('Not using LandIQ before 2009 - skipping')
                continue

            # Computing zonal stats on the EPSG:6414 raster
            # To switch to the UTM zone images, update the LandIQ image/export ID
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

            # TODO: Check what should be the first year to start using LandIQ
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
                logging.debug('Not using LandIQ before 2009 - skipping')
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
                raise Exception(f'unexpected California (LandIQ) year: {year}')

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


    # Then compute zonal stats with LandIQ/CDL composite
    if 'CA' in states:
        state = 'CA'
        logging.info(f'\nCA LandIQ/CDL Composite')

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

            # Select the LandIQ image
            # The pre2009 filtering is handled below when the mosaic is made
            # if year < 2009:
            #     logging.debug('Not using LandIQ before 2009 - skipping')
            #     continue
            if year in [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]:
                # Use the LandIQ directly for years when it is present
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
            # # CGM - Don't need to check cdl_state_years since California 2007
            # #   image is not being used anymore
            # if year < cdl_year_min:
            #     cdl_img_id = f'{cdl_coll_id}/{cdl_year_min}'
            # elif year >= cdl_year_max:
            #     cdl_img_id = f'{cdl_coll_id}/{cdl_year_max}'
            # else:
            #     cdl_img_id = f'{cdl_coll_id}/{year}'
            #     if year not in cdl_state_years[state]:
            #         logging.debug(f'  CDL {year} not available for {state} - skipping')
            #         continue
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
            nlcd_year = min(year, max(nlcd_img_ids.keys()))
            nlcd_year = max(nlcd_year, min(nlcd_img_ids.keys()))
            nlcd_img_id = nlcd_img_ids[nlcd_year]
            nlcd_img = ee.Image(nlcd_img_id).select('landcover')

            # Change any CDL 176 and NLCD 81/82 pixels to 37
            cdl_img = cdl_img.where(
                cdl_img.eq(176).And(nlcd_img.eq(81).Or(nlcd_img.eq(82))), 37
                # cdl_img.eq(176).And(nlcd_img.neq(71)), 37
            )

            # Mosaic the image with LandIQ first
            # For pre2008 images don't use LandIQ
            if year < 2009:
                crop_type_img = cdl_img.reduce(ee.Reducer.firstNonNull())
                crop_source = f'{cdl_img_id} - remapped annual crops'
            else:
                crop_type_img = ee.Image([ca_img, cdl_img]).reduce(ee.Reducer.firstNonNull())
                crop_source = f'CA{ca_img_id.split("/")[-1]} ' \
                              f'CDL{cdl_img_id.split("/")[-1]} composite' \
                              f' - remapped annual crops'

            # Compute zonal stats on the mosaiced images using the LandIQ crs and Transform
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


    # DEADBEEF - Old code for building CDL image stacks
    # # Use the CDL images directly for other states
    # # Is it better to build this as Image or a Collection.toBands()
    # crop_type_img = ee.Image([
    #     ee.Image(f'{cdl_coll_id}/{cdl_year_remap[year]}')
    #         .select(['cropland'], [f'CROP_{year}'])
    #         .uint8()
    #     for year in years])
    # # crop_type_coll = ee.ImageCollection([
    # #     ee.Image(f'{cdl_coll_id}/{year}')
    # #         .select(['cropland'], [f'CROP_{year}'])
    # #     for year in years])
    # # crop_type_img = crop_type_coll.toBands()\
    # #     .rename([f'CROP_{year}' for year in years])

    # Mosaicing the collection could be used to get the b images
    # crop_type_img = ee.Image([
    #     ee.Image(ee.ImageCollection(cdl_coll_id)\
    #         .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
    #         .filterBounds(mgrs_geom)
    #         .mosaic()).rename([f'CROP_{year}'])
    #     for year in years])
    # # crop_type_coll = ee.ImageCollection([
    # #     ee.Image(ee.ImageCollection(crop_type_coll_id)\
    # #         .filterDate(f'{year}-01-01', f'{year+1}-01-01')\
    # #         .filterBounds(mgrs_geom)
    # #         .mosaic()).rename([f'CROP_{year}'])
    # #     for year in years])
    # # crop_type_img = crop_type_coll.toBands()\
    # #     .rename([f'CROP_{year}' for year in years])


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Export field crop type stats files by state',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', nargs='+', required=True,
        help='Comma/space separated list of states')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma/space separated years and/or ranges of years')
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

    main(
        states=args.states,
        years=args.years,
        overwrite_flag=args.overwrite,
        gee_key_file=args.key,
    )
