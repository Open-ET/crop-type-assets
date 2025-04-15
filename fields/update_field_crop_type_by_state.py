import argparse
import json
import logging
import os
import pprint
import re
import subprocess

from google.cloud import storage
from osgeo import ogr
import pandas as pd

import openet.core.utils as utils

ogr.UseExceptions()

# logging.getLogger('googleapiclient').setLevel(logging.INFO)
# logging.getLogger('requests').setLevel(logging.INFO)
# logging.getLogger('urllib3').setLevel(logging.INFO)

STORAGE_CLIENT = storage.Client(project='openet')


def main(states, years=[], overwrite_flag=False):
    """Update field crop type values by state

    Parameters
    ----------
    states : list
    years : list, optional
    overwrite_flag : bool, optional
        If True, overwrite existing crop type values with the new values.

    Returns
    -------

    """
    logging.info('\nUpdating field crop type values by state')

    # Min/max year range to process
    # Don't change min year unless additional CONUS CDL images are ingested
    #   or additional crop type data is identified
    year_min = 1997
    year_max = 2024

    # Years where CDL has full CONUS coverage
    # Don't change min year unless additional CONUS CDL images are ingested
    cdl_year_min = 2008
    cdl_year_max = 2024

    # CSV stats bucket path
    bucket_name = 'openet_geodatabase'
    bucket_folder = 'temp_croptype_20250414'

    output_format = 'CSV'

    shell_flag = True

    field_ws = os.getcwd()
    shapefile_ws = os.path.join(field_ws, 'shapefiles')
    stats_ws = os.path.join(field_ws, output_format.lower())
    if not os.path.isdir(stats_ws):
        os.makedirs(stats_ws)

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
        years = list(range(year_min, year_max+1))
    else:
        years = sorted(list(set(
            int(year) for year_str in years
            for year in utils.str_ranges_2_list(year_str)
            if ((year <= year_max) and (year >= year_min))
        )))
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


    logging.info('\nProcessing CDL crop type by state')
    for state in states:
        # California is processed separately below
        if state == 'CA':
            continue

        logging.info(f'\nState: {state}')
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.debug(f'  {shp_path}')

        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')
            continue

        if overwrite_flag:
            logging.info('\nClearing all crop type and source values')
            shp_driver = ogr.GetDriverByName('ESRI Shapefile')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for input_ftr in input_layer:
                for year in years:
                    input_ftr.SetField(f'CROP_{year}', 0)
                    input_ftr.SetField(f'CSRC_{year}', '')
                input_layer.SetFeature(input_ftr)
            input_ds = None

        # output_path = shp_path.replace('.shp', '_update.shp')
        # if os.path.exists(output_path):
        #     shp_driver.DeleteDataSource(output_path)

        crop_type_fields = [f'CROP_{year}' for year in cdl_state_years[state]]
        logging.debug(f'\nCrop Type Fields: {", ".join(crop_type_fields)}')
        # crop_src_fields = [f'CSRC_{year}' for year in years]
        # logging.debug(f'Fields: {", ".join(crop_src_fields)}')

        logging.info(f'Reading stats {output_format} and updating shapefile (by year)')
        # update_features = {}
        for year in cdl_state_years[state]:
            logging.info(f'{year}')
            stats_name = f'{state}_cdl_{year}.csv'.lower()
            stats_path = os.path.join(stats_ws, stats_name)
            logging.debug(f'  {stats_path}')

            # Only download stats files on overwrite or if not present
            # if overwrite_flag:
            if not os.path.isfile(stats_path) or overwrite_flag:
                logging.debug(f'  Downloading stats {output_format} from bucket')
                if bucket_folder:
                    bucket_path = f'{bucket_folder}/{stats_name}'
                else:
                    bucket_path = f'{stats_name}'
                logging.debug(f'  {bucket_path}')
                src_bucket = STORAGE_CLIENT.bucket(bucket_name)
                src_blob = src_bucket.get_blob(bucket_path)
                src_blob.download_to_filename(stats_path)

            if not os.path.isfile(stats_path):
                logging.info(f'  Stats {output_format} does not exist - skipping')
                continue

            logging.debug(f'  Reading stats {output_format}')
            # Restucture the feature information for writing to the shapefile
            if output_format.upper() == 'CSV':
                update_df = pd.read_csv(stats_path)\
                    .drop(['system:index', '.geo'], axis=1)\
                    .set_index('OPENET_ID')
                update_features = update_df.to_dict('index')
                # print(sum(ftr[f'CROP_{year}'] is None for ftr in update_features.values()))
            elif output_format.upper() == 'GEOJSON':
                with open(stats_path) as f:
                    update_features = json.load(f)
                update_features = {
                    ftr['properties']['OPENET_ID']: {
                        k: 0 if re.match('CROP_\d{4}', k) and v is None else v
                        for k, v in ftr['properties'].items()}
                    for ftr in update_features['features']
                }

            # Log the features that don't have crop types
            for ftr in update_features.values():
                if ftr[f'CROP_{year}'] is None:
                    logging.debug(f'  {ftr["OPENET_ID"]} - crop types is None')
                    # pprint.pprint(ftr)
                    input('ENTER')
                elif ftr[f'CROP_{year}'] == 0:
                    logging.debug(f'  {ftr["OPENET_ID"]} - missing crop types')
                    # pprint.pprint(ftr)
                    # input('ENTER')

            # TODO: Test if writing all years at once is any faster
            logging.debug('  Writing field crop type values')
            write_features(shp_path, update_features, year, overwrite_flag)


    if 'CA' in states:
        logging.info(f'\nProcessing Crop Mapping / CDL crop_type for California')
        state = 'CA'

        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.debug(f'  {shp_path}')

        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')

        if overwrite_flag:
            logging.info('\nClearing all crop type and source values')
            # print(years)
            # input('Press ENTER to continue')
            shp_driver = ogr.GetDriverByName('ESRI Shapefile')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for input_ftr in input_layer:
                for year in years:
                    input_ftr.SetField(f'CROP_{year}', 0)
                    input_ftr.SetField(f'CSRC_{year}', '')
                input_layer.SetFeature(input_ftr)
            input_ds = None

        # First update the shapefile with the California Crop Mapping values
        for year in years:
            if year < 2009:
                continue
            logging.info(f'{year}')

            stats_name = f'{state}_cadwr_{year}.csv'.lower()
            stats_path = os.path.join(stats_ws, stats_name)
            logging.debug(f'  {stats_path}')

            # Only download stats files on overwrite or if not present
            # if not os.path.isfile(stats_path):
            if overwrite_flag or not os.path.isfile(stats_path):
                logging.debug('  Downloading stats file from bucket')
                if bucket_folder:
                    bucket_path = f'{bucket_folder}/{stats_name}'
                else:
                    bucket_path = f'{stats_name}'
                logging.debug(f'  {bucket_path}')
                src_bucket = STORAGE_CLIENT.bucket(bucket_name)
                src_blob = src_bucket.get_blob(bucket_path)
                src_blob.download_to_filename(stats_path)
            if not os.path.isfile(stats_path):
                logging.info('  Stats file does not exist - skipping')
                continue

            logging.debug(f'  Reading stats {output_format}')
            # Restucture the feature information for writing to the shapefile
            if output_format.upper() == 'CSV':
                update_df = (
                    pd.read_csv(stats_path)
                    .drop(['system:index', '.geo'], axis=1)
                    .set_index('OPENET_ID')
                )
                update_features = update_df.to_dict('index')
                # print(sum(ftr[f'CROP_{year}'] is None for ftr in update_features.values()))
            elif output_format.upper() == 'GEOJSON':
                with open(stats_path) as f:
                    update_features = json.load(f)
                update_features = {
                    ftr['properties']['OPENET_ID']: {
                        k: 0 if re.match('CROP_\d{4}', k) and v is None else v
                        for k, v in ftr['properties'].items()}
                    for ftr in update_features['features']
                }

            # Drop features that have less than 50% Crop Mapping coverage
            update_features = {
                k: v for k, v in update_features.items()
                if ((v['PIXEL_TOTAL'] > 0) and (v['PIXEL_COUNT'] / v['PIXEL_TOTAL']) >= 0.50)
            }

            # # Log the features that don't have crop types
            # for ftr in update_features.values():
            #     if ftr[f'CROP_{year}'] is None:
            #         logging.debug(f'  {ftr["OPENET_ID"]} - crop types is None')
            #         # pprint.pprint(ftr)
            #         input('ENTER')
            #     elif ftr[f'CROP_{year}'] == 0:
            #         logging.debug(f'  {ftr["OPENET_ID"]} - missing crop types')
            #         # pprint.pprint(ftr)
            #         # input('ENTER')

            # TODO: Test if writing all years at once is any faster
            logging.debug('  Writing field crop type values')
            write_features(shp_path, update_features, year, overwrite_flag)


        # Then update any missing values with the CA/CDL composite values
        for year in years:
            if year < 2008:
                continue
            logging.info(f'{year}')

            stats_name = f'{state}_composite_{year}.csv'.lower()
            stats_path = os.path.join(stats_ws, stats_name)
            logging.debug(f'  {stats_path}')

            # Only download stats files on overwrite or if not present
            # if overwrite_flag or not os.path.isfile(stats_path):
            if not os.path.isfile(stats_path):
                logging.info('  Downloading stats files from bucket')
                if bucket_folder:
                    bucket_path = f'{bucket_folder}/{stats_name}'
                else:
                    bucket_path = f'{stats_name}'
                logging.debug(f'  {bucket_path}')
                src_bucket = STORAGE_CLIENT.bucket(bucket_name)
                src_blob = src_bucket.get_blob(bucket_path)
                src_blob.download_to_filename(stats_path)
            if not os.path.isfile(stats_path):
                logging.info('  Stats file does not exist - skipping')
                continue

            logging.debug(f'  Reading stats {output_format}')
            # Restucture the feature information for writing to the shapefile
            if output_format.upper() == 'CSV':
                update_df = (
                    pd.read_csv(stats_path)
                    .drop(['system:index', '.geo'], axis=1)
                    .set_index('OPENET_ID')
                )
                update_features = update_df.to_dict('index')
                # print(sum(ftr[f'CROP_{year}'] is None for ftr in update_features.values()))
            elif output_format.upper() == 'GEOJSON':
                with open(stats_path) as f:
                    update_features = json.load(f)
                update_features = {
                    ftr['properties']['OPENET_ID']: {
                        k: 0 if re.match('CROP_\d{4}', k) and v is None else v
                        for k, v in ftr['properties'].items()}
                    for ftr in update_features['features']
                }

            # Log the features that don't have crop types
            for ftr in update_features.values():
                if ftr[f'CROP_{year}'] is None:
                    logging.debug(f'  {ftr["OPENET_ID"]} - crop types is None')
                    # pprint.pprint(ftr)
                    input('ENTER')
                elif ftr[f'CROP_{year}'] == 0:
                    logging.debug(f'  {ftr["OPENET_ID"]} - missing crop types')
                    # pprint.pprint(ftr)
                    # input('ENTER')

            # TODO: Test if writing all years at once is any faster
            logging.debug('  Writing field crop type values')
            write_features(shp_path, update_features, year, overwrite=False)


def write_features(shp_path, features, year, overwrite=False):
    """Update crop type/source for a single year"""
    shp_driver = ogr.GetDriverByName('ESRI Shapefile')
    output_ds = shp_driver.Open(shp_path, 1)
    output_layer = output_ds.GetLayer()
    crop_type_field = f'CROP_{year}'
    crop_src_field = f'CSRC_{year}'
    for output_ftr in output_layer:
        output_id = output_ftr.GetField('OPENET_ID')
        crop_type = output_ftr.GetField(crop_type_field)
        # crop_src = output_ftr.GetField(crop_src_field)

        try:
            values = features[output_id]
        except KeyError:
            continue
        try:
            new_crop_type = int(values[crop_type_field])
        except:
            new_crop_type = 0
        try:
            new_crop_src = values[crop_src_field]
        except:
            new_crop_src = ''

        if new_crop_type == 0:
            continue
        elif crop_type > 0 and not overwrite:
            continue
        output_ftr.SetField(crop_type_field, new_crop_type)
        output_ftr.SetField(crop_src_field, new_crop_src)
        output_layer.SetFeature(output_ftr)
    output_ds = None


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Update field crop type values by state',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', default=['ALL'], nargs='+',
        help='Comma/space separated list of states')
    parser.add_argument(
        '--years', default='', nargs='+',
        help='Comma/space separated years and/or ranges of years')
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

    main(states=args.states, years=args.years, overwrite_flag=args.overwrite)
