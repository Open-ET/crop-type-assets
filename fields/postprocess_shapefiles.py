import argparse
# from datetime import datetime, timezone
import logging
import os
import pprint
import subprocess
from time import sleep
import zipfile

import ee
from google.cloud import storage

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

STORAGE_CLIENT = storage.Client(project='openet')


def main(states, overwrite_flag=False, gee_key_file=None, project_id=None):
    """Postprocess and upload the state field shapefiles

    Parameters
    ----------
    states : list
    overwrite_flag : bool, optional
    gee_key_file : str, None, optional
        Earth Engine service account JSON key file (the default is None).
    project_id : str, optional
        Google cloud project ID to use for GEE authentication.
        This will be checked after the gee_key_file and before the user Initialize.
        The default is None.

    """
    logging.info('\nZip the state field shapefiles')

    field_ws = os.getcwd()
    shapefile_ws = os.path.join(field_ws, 'shapefiles')
    output_zip_ws = os.path.join(field_ws, 'updated_zips')

    bucket_name = 'openet_field_boundaries'
    bucket_folder = ''

    # For now write the fields to a temp folder
    collection_folder = f'projects/openet/assets/features/fields/temp'

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

    if not os.path.isdir(output_zip_ws):
        os.makedirs(output_zip_ws)

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


    logging.info('\nReading bucket files')
    bucket = STORAGE_CLIENT.bucket(bucket_name)
    bucket_files = sorted([x.name for x in bucket.list_blobs()])


    for state in states:
        logging.info(f'{state}')

        # TODO: logging.info('Removing unused fields')

        logging.info('Zipping state shapefiles')
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        zip_name = f'{state}.zip'
        zip_path = os.path.join(output_zip_ws, f'{state}.zip')
        logging.debug(f'  {shp_path}')
        logging.debug(f'  {zip_path}')
        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')
            continue
        elif not overwrite_flag and os.path.isfile(zip_path):
            logging.info('  Zip exists and overwrite is False - skipping')
            continue
        with zipfile.ZipFile(zip_path, 'w') as zip:
            for file_name in os.listdir(os.path.join(shapefile_ws, state)):
                if file_name.startswith(state):
                    zip.write(
                        os.path.join(shapefile_ws, state, file_name),
                        arcname=f'{state}/{file_name}'
                    )

        if not os.path.isfile(zip_path):
            logging.info('  Zip file does not exist - skipping')
            continue


        logging.info('Uploading zip file to bucket')
        bucket_path = f'{bucket_folder}/{zip_name}' if bucket_folder else zip_name
        logging.debug(f'  {bucket_path}')
        blob = bucket.blob(bucket_path)
        # if blob.exists():
        if zip_name in bucket_files:
            if overwrite_flag:
                logging.info('  Removing existing zip file')
                blob.delete()
            else:
                logging.info('  Overwrite is False - skipping')
                continue
        blob.upload_from_filename(zip_path)
        # if bucket_folder:
        #     upload_path = f'gs://{bucket_name}/{bucket_folder}/'
        # else:
        #     upload_path = f'gs://{bucket_name}/'
        # subprocess.call(
        #     ['gsutil', 'cp', zip_path, upload_path],
        #     # cwd=field_ws,
        #     # shell=shell_flag,
        # )
        sleep(5)


        logging.info('Ingesting shapefiles into Earth Engine')
        # for state in states:
        # logging.info(f'{state}')
        bucket_path = f'gs://{bucket_name}/{bucket_folder}/{state}.zip'
        collection_id = f'{collection_folder}/{state}'
        logging.debug(f'  {bucket_path}')
        logging.debug(f'  {collection_id}')

        if ee.data.getInfo(collection_id):
            if overwrite_flag:
                logging.info('  FeatureCollection already exists - removing')
                ee.data.deleteAsset(collection_id)
            else:
                logging.info('  FeatureCollection already exists - skipping')
                continue

        logging.info('  Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': collection_id,
            'sources': [{'primaryPath': bucket_path}],
            # 'properties': {
            #     date_property: datetime.today().strftime('%Y-%m-%d'),
            # }
        }
        try:
            ee.data.startTableIngestion(task_id, params, allow_overwrite=True)
        except Exception as e:
            logging.exception(f'  Exception: {e}\n  Exiting')
            return False


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Update field crop type values by feature',
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
