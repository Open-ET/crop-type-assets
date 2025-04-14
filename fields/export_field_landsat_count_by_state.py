import argparse
import logging
import os
import pprint

import ee
from google.cloud import storage

import openet.core.utils as utils

STORAGE_CLIENT = storage.Client(project='openet')

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)


def main(states, overwrite_flag=False, gee_key_file=None, project_id=None):
    """Export field crop type geojson by state

    Parameters
    ----------
    states : list
    overwrite_flag : bool, optional
        If True, overwrite existing files (the default is False).
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).

    Returns
    -------

    """
    logging.info('\nExport field landsat count stats by state')

    field_folder_id = f'projects/openet/assets/features/fields/temp'
    # field_folder_id = f'projects/openet/assets/features/fields/2024-02-01'

    bucket_name = 'openet'
    bucket_folder = 'crop_type/pixelcount'

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
        input('ENTER')

    logging.info('\nGetting bucket file list')
    bucket = STORAGE_CLIENT.get_bucket(bucket_name)
    bucket_files = sorted([
        x.name.replace(bucket_folder + '/', '')
        for x in bucket.list_blobs(prefix=bucket_folder + '/')
        if x.name.replace(bucket_folder + '/', '')
    ])


    for state in states:
        logging.info(f'\n{state} CDL')

        field_coll_id = f'{field_folder_id}/{state}'
        mgrs_tiles = utils.get_info(
            ee.FeatureCollection(field_coll_id).aggregate_histogram('MGRS_TILE').keys()
        )
        utm_zones = {mgrs_tile[:2] for mgrs_tile in mgrs_tiles}

        for utm_zone in utm_zones:
            export_id = f'{state}_landsat_utm{utm_zone}'.lower()
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

            def pixel_count(ftr):
                output = (
                    ee.Image.constant(1)
                    .reduceRegion(
                        reducer=ee.Reducer.sum().unweighted(),
                        geometry=ee.Feature(ftr).geometry(),
                        crs=f'EPSG:326{utm_zone}',
                        crsTransform=[30, 0, 15, 0, -30, 15],
                        bestEffort=False,
                    )
                )
                return ee.Feature(
                    None,
                    {
                        'OPENET_ID': ftr.get('OPENET_ID'),
                        'PIXELCOUNT': output.get('constant'),
                        'UTM_ZONE': utm_zone,
                    }
                )
            count_coll = (
                ee.FeatureCollection(field_coll_id)
                .filter(ee.Filter.stringStartsWith('MGRS_TILE', utm_zone))
                .map(pixel_count)
            )
            # pprint.pprint(count_coll.first().getInfo())
            # input('ENTER')

            # logging.debug('  Building export task')
            task = ee.batch.Export.table.toCloudStorage(
                collection=count_coll,
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
        description='Export field landsat count stats by state',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', default=['ALL'], nargs='+',
        help='Comma/space separated list of states')
    parser.add_argument(
        '--overwrite', default=False, action='store_true',
        help='Force overwrite of existing files')
    parser.add_argument(
        '--key', type=utils.arg_valid_file, metavar='FILE',
        help='GEE service account key file')
    parser.add_argument(
        '--project', default=None,
        help='Google cloud project ID to use for GEE authentication')
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
        overwrite_flag=args.overwrite,
        gee_key_file=args.key,
        project_id=args.project,
    )
