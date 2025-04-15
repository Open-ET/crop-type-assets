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


def main(states, overwrite_flag=False):
    """Update field crop type values by state

    Parameters
    ----------
    states : list
    overwrite_flag : bool, optional
        If True, overwrite existing crop type values with the new values.

    Returns
    -------

    """
    logging.info('\nUpdating field landsat count stats by state')

    output_format = 'CSV'

    # CSV stats bucket path
    bucket_name = 'openet'
    bucket_folder = 'crop_type/pixelcount'

    openet_id_field = 'OPENET_ID'
    pixel_count_field = 'PIXELCOUNT'

    shp_driver = ogr.GetDriverByName('ESRI Shapefile')

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

    logging.info('\nGetting bucket file list')
    bucket = STORAGE_CLIENT.get_bucket(bucket_name)
    bucket_files = sorted([
        x.name.replace(bucket_folder + '/', '')
        for x in bucket.list_blobs(prefix=bucket_folder + '/')
        if x.name.replace(bucket_folder + '/', '')
    ])
    bucket_files = [f for f in bucket_files if 'utm' in f]


    for state in states:
        logging.info(f'\nState: {state}')
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.debug(f'  {shp_path}')

        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')
            continue

        if overwrite_flag:
            logging.info('\nClearing all PIXELCOUNT values')
            shp_driver = ogr.GetDriverByName('ESRI Shapefile')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for input_ftr in input_layer:
                input_ftr.SetField(f'PIXELCOUNT', 0)
                input_layer.SetFeature(input_ftr)
            input_ds = None

        logging.info(f'Reading stats {output_format} and updating shapefile')
        # update_features = {}

        for utm_zone in range(10, 20):
            stats_name = f'{state}_landsat_utm{utm_zone}.csv'.lower()
            stats_path = os.path.join(stats_ws, stats_name)
            if stats_name not in bucket_files:
                continue
            logging.info(f'  {stats_name}')
            logging.debug(f'  {stats_path}')

            # Only download stats files on overwrite or if not present
            # if overwrite_flag:
            if not os.path.isfile(stats_path) or overwrite_flag:
                logging.debug(f'  Downloading stats {output_format} from bucket')
                if bucket_folder:
                    bucket_path = f'{bucket_folder}/{stats_name}'
                else:
                    bucket_path = f'{stats_name}'
                src_bucket = STORAGE_CLIENT.bucket(bucket_name)
                src_blob = src_bucket.get_blob(bucket_path)
                src_blob.download_to_filename(stats_path)

            if not os.path.isfile(stats_path):
                logging.info(f'  Stats {output_format} does not exist - skipping')
                continue

            logging.debug(f'  Reading stats {output_format}')
            # Restructure the feature information for writing to the shapefile
            if output_format.upper() == 'CSV':
                update_df = (
                    pd.read_csv(stats_path)
                    .drop(['system:index', '.geo'], axis=1)
                    .set_index('OPENET_ID')
                )
                update_features = update_df.to_dict('index')
                # print(sum(ftr['PIXELCOUNT'] is None for ftr in update_features.values()))
            elif output_format.upper() == 'GEOJSON':
                with open(stats_path) as f:
                    update_features = json.load(f)
                update_features = {
                    ftr['properties']['OPENET_ID']: {
                        k: 0 if re.match('PIXELCOUNT', k) and v is None else v
                        for k, v in ftr['properties'].items()}
                    for ftr in update_features['features']
                }
            # pprint.pprint(update_features)
            # input('ENTER')

            # Log the features that don't have pixel counts
            for ftr in update_features.values():
                if ftr[f'PIXELCOUNT'] is None:
                    logging.debug(f'  {ftr["OPENET_ID"]} - pixel count is None')
                    # pprint.pprint(ftr)
                    input('ENTER')

            logging.debug('  Writing field crop type values')
            output_ds = shp_driver.Open(shp_path, 1)
            output_layer = output_ds.GetLayer()
            for output_ftr in output_layer:
                output_id = output_ftr.GetField(openet_id_field)
                try:
                    output_ftr.SetField(
                        pixel_count_field,
                        int(round(update_features[output_id]['PIXELCOUNT']))
                    )
                except:
                    continue
                output_layer.SetFeature(output_ftr)
            output_ds = None


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Update field landsat count stats by state',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', default=['ALL'], nargs='+',
        help='Comma/space separated list of states')
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

    main(states=args.states, overwrite_flag=args.overwrite)
