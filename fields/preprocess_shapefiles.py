import argparse
from datetime import datetime, timezone
import logging
import math
import os
import pprint
import re
import shutil
import subprocess
import zipfile

import ee
from google.cloud import storage
from osgeo import ogr, osr

import openet.core.utils as utils

ogr.UseExceptions()

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

STORAGE_CLIENT = storage.Client(project='openet')


def main(states, years=[], overwrite_flag=False, gee_key_file=None, project_id=None):
    """Download and preprocess the state field shapefiles

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

    """
    logging.info('\nUpdating field crop type values')

    year_min = 1997
    year_max = 2024

    field_ws = os.getcwd()
    input_zip_ws = os.path.join(field_ws, 'source_zips')
    shapefile_ws = os.path.join(field_ws, 'shapefiles')

    bucket_name = 'openet_geodatabase'
    bucket_folder = 'temp_shp_20250409'

    # For now write the fields to a temp folder
    collection_folder = f'projects/openet/assets/features/fields/temp'

    if states == ['ALL']:
        # 'AL' is not included since there is not an Alabama field shapefile
        # 'MX' is Mexico (but only includes Mexicali for now)
        # Might want to switch to Mexican state names/abbreviations at some point
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

    crop_type_fields = [f'CROP_{year}' for year in years]
    crop_src_fields = [f'CSRC_{year}' for year in years]
    logging.debug(f'Fields: {", ".join(crop_type_fields)}')
    logging.debug(f'Fields: {", ".join(crop_src_fields)}')

    shp_driver = ogr.GetDriverByName('ESRI Shapefile')

    count_field = 'PIXELCOUNT'
    area_field = 'SHAPE_AREA'
    length_field = 'SHAPE_LENG'
    score_field = 'PP_SCORE'

    delete_fields = [
        'fid',
        'layer', 'path',
        'Shape_length', 'Shape_area', 'Acres',
        'Shape_Leng', 'Shape_Area', 'PP_Score', 'PXL_COUNT', 'PP',
        'SHAPE_LENG', 'SHAPE_AREA', 'PP_SCORE', 'PIXELCOUNT',
        'STATE',
    ]

    # mgrs_tile_field = 'MGRS_TILE'
    # mgrs_zone_field = 'MGRS_ZONE'

    area_osr = osr.SpatialReference()
    area_osr.ImportFromProj4('+proj=aea +lat_0=23 +lon_0=-96 +lat_1=29.5 +lat_2=45.5 +x_0=0 +y_0=0 '
                             '+datum=NAD83 +units=m +no_defs +type=crs')


    # CGM - I think Matt used a 2000m2 area threshold, so start smaller than that initially
    area_threshold = 1000
    # area_threshold = 2000

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


    # # Build the export collection if it doesn't exist
    # if not ee.data.getInfo(collection_folder):
    #     logging.info(f'\nFolder does not exist and will be built\n  {collection_folder}')
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'FOLDER'}, collection_folder)
    #
    # logging.info('\nIngest shapefiles into Earth Engine')
    # for state in states:
    #     logging.info(f'State: {state}')
    #     if bucket_folder:
    #         bucket_path = f'gs://{bucket_name}/{bucket_folder}/{state}.zip'
    #     else:
    #         bucket_path = f'gs://{bucket_name}/{state}.zip'
    #     collection_id = f'{collection_folder}/{state}'
    #     logging.debug(f'  {bucket_path}')
    #     logging.debug(f'  {collection_id}')
    #
    #     if ee.data.getInfo(collection_id):
    #         if overwrite_flag:
    #             logging.info('  FeatureCollection already exists - removing')
    #             ee.data.deleteAsset(collection_id)
    #         else:
    #             logging.info('  FeatureCollection already exists - skipping')
    #             continue
    #
    #     logging.info('  Ingesting into Earth Engine')
    #     # task_id = ee.data.newTaskId()[0]
    #     # logging.debug(f'  {task_id}')
    #     # params = {
    #     #     'name': collection_id,
    #     #     'sources': [{'primaryPath': bucket_path}],
    #     #     # 'properties': {
    #     #     #     date_property: datetime.today().strftime('%Y-%m-%d')
    #     #     # }
    #     # }
    #     # try:
    #     #     ee.data.startTableIngestion(task_id, params, allow_overwrite=True)
    #     # except Exception as e:
    #     #     logging.exception(f'  Exception: {e}\n  Exiting')
    #     #     return False
    #
    #     # MB - The ee.data.startTableIngestion call wasn't working consistently.
    #     # Trying to use the EE commandline tool
    #     try:
    #         args = [
    #             'earthengine', 'upload', 'table',
    #             f'--asset_id=projects/openet/featureCollections/temp/{state}',
    #             bucket_path
    #         ]
    #         if overwrite_flag:
    #             args.append('--force')
    #         subprocess.call(
    #             args,
    #             # shell=shell_flag
    #         )
    #     except Exception as e:
    #         logging.exception(f'  Exception: {e}\n  Exiting')
    #         return False


    # # Download the field shapefiles from the bucket
    # # Equivalent to: gsutil -m cp gs://openet_field_boundaries/*.zip ./source_zips/
    # logging.info('\nDownloading shapefiles from bucket')
    # if not os.path.isdir(input_zip_ws):
    #     os.makedirs(input_zip_ws)
    # for state in states:
    #     logging.info(f'State: {state}')
    #     if bucket_folder:
    #         bucket_path = f'{bucket_folder}/{state}.zip'
    #     else:
    #         bucket_path = f'{state}.zip'
    #     zip_path = os.path.join(input_zip_ws, f'{state}.zip')
    #     logging.debug(f'  {bucket_path}')
    #     logging.debug(f'  {zip_path}')
    #     if os.path.isfile(zip_path) and not overwrite_flag:
    #         logging.info('  zip file already exists - skipping')
    #         continue
    #     src_bucket = STORAGE_CLIENT.bucket(bucket_name)
    #     src_blob = src_bucket.get_blob(bucket_path)
    #     src_blob.download_to_filename(zip_path)


    # logging.info('\nExtracting shapefiles from zips')
    # for state in states:
    #     logging.info(f'State: {state}')
    #     zip_path = os.path.join(input_zip_ws, f'{state}.zip')
    #     state_ws = os.path.join(shapefile_ws, state)
    #     shp_path = os.path.join(state_ws, f'{state}.shp')
    #     logging.debug(f'  {zip_path}')
    #     logging.debug(f'  {shp_path}')
    #
    #     if not os.path.isdir(state_ws):
    #         os.makedirs(state_ws)
    #     if not os.path.isfile(zip_path):
    #         logging.info('  zip file does not exist - skipping')
    #         continue
    #     elif os.path.isfile(shp_path) and not overwrite_flag:
    #         logging.info('  shp file already exists - skipping')
    #         continue
    #
    #     # Assume other files are present if .shp is present
    #     with zipfile.ZipFile(zip_path) as zf:
    #         zf.extractall(shapefile_ws)
    #
    #     # Check if the shapefile was extracted to the root
    #     if os.path.isfile(os.path.join(shapefile_ws, f'{state}.shp')):
    #         logging.info('  shapefile extracted to root - moving to state folder')
    #         for ext in ['shp', 'shx', 'dbf', 'prj']:
    #             shutil.move(os.path.join(shapefile_ws, f'{state}.{ext}'),
    #                         os.path.join(state_ws, f'{state}.{ext}'))
    #         for ext in ['cpg', 'qpj', 'sbx', 'sbn', 'shp.xml']:
    #             if os.path.isfile(os.path.join(shapefile_ws, f'{state}.{ext}')):
    #                 os.remove(os.path.join(shapefile_ws, f'{state}.{ext}'))


    logging.info('\nProcessing crop type by state')
    for state in states:
        logging.info(f'\nState: {state}')

        state_ws = os.path.join(shapefile_ws, state)
        shp_path = os.path.join(state_ws, f'{state}.shp')
        logging.info(f'  {shp_path}')

        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')
            continue

        # Remove extra sidecar files
        for ext in ['cpg', 'qpj', 'sbx', 'sbn', 'shp.xml']:
            if os.path.isfile(os.path.join(state_ws, f'{state}.{ext}')):
                os.remove(os.path.join(state_ws, f'{state}.{ext}'))
            if os.path.isfile(os.path.join(shapefile_ws, f'{state}.{ext}')):
                os.remove(os.path.join(shapefile_ws, f'{state}.{ext}'))

        if overwrite_flag:
            logging.info('  Removing existing crop type/source fields')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            input_lyr_defn = input_layer.GetLayerDefn()
            input_fields = [
                [i, input_lyr_defn.GetFieldDefn(i).GetNameRef()]
                for i in range(input_lyr_defn.GetFieldCount())
            ]
            # Delete the fields in reverse order
            for field_i, field_name in input_fields[::-1]:
                if re.match('(CROP|CSRC)_\d{4}', field_name):
                    logging.debug(f'  {field_name}')
                    input_layer.DeleteField(field_i)
                elif re.match('CDL_\d{4}', field_name):
                    logging.debug(f'  {field_name}')
                    input_layer.DeleteField(field_i)
                elif field_name in delete_fields:
                    logging.debug(f'  {field_name}')
                    input_layer.DeleteField(field_i)
            input_ds = None

            # Check the field list after removing existing fields
            input_ds = shp_driver.Open(shp_path, 0)
            input_layer = input_ds.GetLayer()
            input_lyr_defn = input_layer.GetLayerDefn()
            input_fields = [
                input_lyr_defn.GetFieldDefn(i).GetNameRef()
                for i in range(input_lyr_defn.GetFieldCount())
            ]
            input_ds = None

            # Reorder the non-crop fields
            # Put the fid and OPENET_ID at the front
            if 'OPENET_ID' not in input_fields:
                print('  ID field not present - skipping state')
                continue
            reordered_fields = []
            reordered_fields.append('OPENET_ID')
            if 'SOURCECODE' in input_fields:
                reordered_fields.append('SOURCECODE')
            if 'MOD_DATE' in input_fields:
                reordered_fields.append('MOD_DATE')
            if 'FIPS' in input_fields:
                reordered_fields.append('FIPS')
            if 'HUC12' in input_fields:
                reordered_fields.append('HUC12')
            if 'MGRS_TILE' in input_fields:
                reordered_fields.append('MGRS_TILE')

            if reordered_fields == input_fields:
                print(f'  Fields are in order')
            elif set(reordered_fields) != set(input_fields):
                print(f'  Field lists are not consistent, skipping state')
                print(input_fields)
                print(reordered_fields)
                input('ENTER')
                continue
            else:
                print(f'  Reording fields')
                input_ds = shp_driver.Open(shp_path, 1)
                input_layer = input_ds.GetLayer()
                input_lyr_defn = input_layer.GetLayerDefn()
                input_fields = [
                    input_lyr_defn.GetFieldDefn(i).GetNameRef()
                    for i in range(input_lyr_defn.GetFieldCount())
                ]
                # field_index = list(range(len(input_fields)))
                field_index = [input_fields.index(f) for f in reordered_fields]
                input_layer.ReorderFields(field_index)
                input_ds = None


        # Get the updated field list
        input_ds = shp_driver.Open(shp_path, 0)
        input_layer = input_ds.GetLayer()
        input_lyr_defn = input_layer.GetLayerDefn()
        existing_fields = [
            input_lyr_defn.GetFieldDefn(i).GetNameRef()
            for i in range(input_lyr_defn.GetFieldCount())
        ]
        input_ds = None

        # Add ancillary fields if not present
        for f_name, f_size, f_type in [
                ['FIPS', 5, 'string'],
                ['HUC12', 12, 'string'],
                ['MGRS_TILE', 5, 'string'],
                ['MOD_DATE', 10, 'string'],
                ['SOURCECODE', 80, 'string'],
                ['STATE', 2, 'string'],
                ]:
            if f_name not in existing_fields:
                logging.info(f'  Adding {f_name} field')
                input_ds = shp_driver.Open(shp_path, 1)
                input_layer = input_ds.GetLayer()
                # input_lyr_defn = input_layer.GetLayerDefn()
                field_defn = ogr.FieldDefn(f_name, ogr.OFTString)
                field_defn.SetWidth(f_size)
                input_layer.CreateField(field_defn)
                for input_ftr in input_layer:
                    if f_name == 'MOD_DATE':
                        input_ftr.SetField('MOD_DATE', datetime.today().strftime('%Y-%m-%d'))
                    elif f_name == 'STATE':
                        input_ftr.SetField('STATE', state.upper())
                    # else:
                    #     input_ftr.SetField(f_name, '')
                    input_layer.SetFeature(input_ftr)
                input_ds = None

        # Add the area, length, and score fields if not present
        for f_name, f_type in [
                [area_field, 'real'],
                [length_field, 'real'],
                [score_field, 'real'],
                [count_field, 'integer'],
                ]:
            if f_name not in existing_fields:
                logging.info(f'  Adding {f_name} field')
                input_ds = shp_driver.Open(shp_path, 1)
                input_layer = input_ds.GetLayer()
                # input_lyr_defn = input_layer.GetLayerDefn()
                if f_type == 'real':
                    field_defn = ogr.FieldDefn(f_name, ogr.OFTReal)
                    field_defn.SetWidth(24)
                    field_defn.SetPrecision(2)
                elif f_type == 'integer':
                    field_defn = ogr.FieldDefn(f_name, ogr.OFTInteger)
                else:
                    raise ValueError(f'unsupported type: {f_type}')
                input_layer.CreateField(field_defn)

                for input_ftr in input_layer:
                    # if f_name == count_field:
                    #     input_ftr.SetField(f_name, 0)
                    input_ftr.SetField(f_name, 0)
                    input_layer.SetFeature(input_ftr)
                input_ds = None


        # Compute the area/length/score
        if overwrite_flag:
            logging.info(f'  Computing area, length, and PP score')
            input_ds = shp_driver.Open(shp_path, 0)
            input_layer = input_ds.GetLayer()
            # input_osr = osr.SpatialReference()
            input_osr = input_layer.GetSpatialRef()
            # input_osr = str(input_osr.ExportToWkt())
            transform = osr.CoordinateTransformation(input_osr, area_osr)
            input_geometries = {}
            input_areas = {}
            input_lengths = {}
            for input_ftr in input_layer:
                # input_fid = input_ftr.GetFID()
                input_id = input_ftr.GetField('OPENET_ID')
                try:
                    input_geom = input_ftr.GetGeometryRef().Clone()
                except:
                    print(f'  {input_id} - no geometry')
                    continue
                # input_geom.FlattenTo2D()
                if not input_geom:
                    print(f'  {input_id} - no geometry')
                elif not input_geom.IsValid():
                    print(f'  {input_id} - invalid geometry')
                input_geometries[input_id] = input_geom
            inupt_ds = None

            # Compute the area
            for input_id, input_geom in input_geometries.items():
                input_geom.Transform(transform)
                input_areas[input_id] = input_geom.GetArea()
                input_lengths[input_id] = input_geom.Boundary().Length()

                if input_areas[input_id] < area_threshold:
                    print(f'  {input_id} - {input_areas[input_id]}')

            # Write the area values back to the shapefile
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for input_ftr in input_layer:
                # input_fid = input_ftr.GetFID()
                input_id = input_ftr.GetField('OPENET_ID')
                input_ftr.SetField(area_field, round(input_areas[input_id], 2))
                input_ftr.SetField(length_field, round(input_lengths[input_id], 2))
                # Score is computed as the polygon's area by 4pi dividing by the perimeter squared
                input_ftr.SetField(score_field, input_areas[input_id] * math.pi * 4 / (input_lengths[input_id] ** 2))
                input_layer.SetFeature(input_ftr)
            input_ds = None


        # Write the crop type and source fields
        new_crop_type_fields = [f for f in crop_type_fields if f not in existing_fields]
        new_crop_src_fields = [f for f in crop_src_fields if f not in existing_fields]
        if not new_crop_type_fields and not new_crop_src_fields:
            logging.info('  No new fields to add')
            # logging.info('  No new fields to add - skipping')
            # continue

        # Add new crop type fields and set value to 0
        if new_crop_type_fields:
            logging.info(f'  Adding crop type fields: {", ".join(new_crop_type_fields)}')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for crop_type_field in new_crop_type_fields:
                field_defn = ogr.FieldDefn(crop_type_field, ogr.OFTInteger)
                input_layer.CreateField(field_defn)
                existing_fields.append(crop_type_field)
            input_ds = None

        # Add new crop source fields and set value to ''
        if new_crop_src_fields:
            logging.info(f'  Adding crop source fields: {", ".join(new_crop_src_fields)}')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for crop_src_field in new_crop_src_fields:
                field_defn = ogr.FieldDefn(crop_src_field, ogr.OFTString)
                field_defn.SetWidth(64)
                input_layer.CreateField(field_defn)
                existing_fields.append(crop_src_field)
            input_ds = None

        if new_crop_type_fields or new_crop_src_fields:
            logging.info('  Setting new crop type/source default field values')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            for input_ftr in input_layer:
                for crop_type_field in new_crop_type_fields:
                    input_ftr.SetField(crop_type_field, 0)
                for crop_src_field in new_crop_src_fields:
                    input_ftr.SetField(crop_src_field, '')
                input_layer.SetFeature(input_ftr)
            input_ds = None

        # # Get the field list again to check if the fields are sorted
        # input_ds = shp_driver.Open(shp_path, 0)
        # input_layer = input_ds.GetLayer()
        # input_lyr_defn = input_layer.GetLayerDefn()
        # existing_fields = [
        #     input_lyr_defn.GetFieldDefn(i).GetNameRef()
        #     for i in range(input_lyr_defn.GetFieldCount())]
        # input_ds = None


        # Check if the crop type/source fields are in order
        crop_fields = [f for f in existing_fields if re.match('(CROP|CSRC)_\d{4}', f)]
        if crop_fields != sorted(crop_fields):
            logging.info('  Crop type/source fields are out of order')
            # input('  Press ENTER to sort the crop fields')

            # Put the crop type/source fields in order
            # This will move non crop fields up to the front but keep
            #   them in their respective order
            logging.info('  Reordering the crop type/source fields')
            input_ds = shp_driver.Open(shp_path, 1)
            input_layer = input_ds.GetLayer()
            input_lyr_defn = input_layer.GetLayerDefn()
            fields = [
                input_lyr_defn.GetFieldDefn(i).GetNameRef()
                for i in range(input_lyr_defn.GetFieldCount())
            ]
            crop_fields = sorted([
                [field, i] for i, field in enumerate(fields)
                if re.match('(CROP|CSRC)_\d{4}', field)
            ])
            crop_index = {field: i for field, i in crop_fields}
            field_index = [
                i for i, field in enumerate(fields)
                if not re.match('(CROP|CSRC)_\d{4}', field)
            ]
            for field, i in crop_fields:
                field_index.append(crop_index[field])
            input_layer.ReorderFields(field_index)
            input_ds = None


        # Check for missing or duplicate OPENET_ID values
        logging.info('  Checking for missing/duplicate OPENET_ID values')
        input_ds = shp_driver.Open(shp_path, 0)
        input_layer = input_ds.GetLayer()
        openet_id_set = set()
        for input_ftr in input_layer:
            fid = input_ftr.GetFID()
            openet_id = input_ftr.GetField('OPENET_ID')
            if openet_id is None:
                print(f'No ID value for FID {fid}')
            elif openet_id in openet_id_set:
                print(f'Duplicate ID {openet_id}')
            else:
                openet_id_set.add(openet_id)
        input_ds = None


        # # DEADBEEF
        # logging.info('  Removing existing crop source fields')
        # input_ds = shp_driver.Open(shp_path, 1)
        # input_layer = input_ds.GetLayer()
        # input_lyr_defn = input_layer.GetLayerDefn()
        # input_fields = [
        #     [i, input_lyr_defn.GetFieldDefn(i).GetNameRef()]
        #     for i in range(input_lyr_defn.GetFieldCount())
        # ]
        # # Delete the fields in reverse order
        # for field_i, field_name in input_fields[::-1]:
        #     if re.match('(CSRC)_\d{4}', field_name):
        #         logging.debug(f'  {field_name}')
        #         input_layer.DeleteField(field_i)
        # input_ds = None

        # # DEADBEEF - Code for adding the MGRS_ZONE field after MGRS_TILE
        # logging.info(f'  Adding MGRS_ZONE field')
        # input_ds = shp_driver.Open(shp_path, 1)
        # input_layer = input_ds.GetLayer()
        # input_lyr_defn = input_layer.GetLayerDefn()
        # fields = [
        #     input_lyr_defn.GetFieldDefn(i).GetNameRef()
        #     for i in range(input_lyr_defn.GetFieldCount())
        # ]
        # if 'MGRS_ZONE' not in fields:
        #     field_defn = ogr.FieldDefn('MGRS_ZONE', ogr.OFTString)
        #     field_defn.SetWidth(3)
        #     input_layer.CreateField(field_defn)
        #     for input_ftr in input_layer:
        #         input_ftr.SetField('MGRS_ZONE', input_ftr.GetField('MGRS_TILE')[:3])
        #         input_layer.SetFeature(input_ftr)
        # input_ds = None
        # input_ds = shp_driver.Open(shp_path, 1)
        # input_layer = input_ds.GetLayer()
        # input_lyr_defn = input_layer.GetLayerDefn()
        # fields = [
        #     input_lyr_defn.GetFieldDefn(i).GetNameRef()
        #     for i in range(input_lyr_defn.GetFieldCount())
        # ]
        # field_index = list(range(len(fields)))
        # field_index.insert(fields.index('MGRS_TILE')+1,
        #                    field_index.pop(fields.index('MGRS_ZONE')))
        # input_layer.ReorderFields(field_index)
        # input_ds = None

        # # DEADBEEF - Code for removing the MGRS_ZONE field
        # logging.info('  Removing MGRS_ZONE field')
        # input_ds = shp_driver.Open(shp_path, 1)
        # input_layer = input_ds.GetLayer()
        # input_lyr_defn = input_layer.GetLayerDefn()
        # input_fields = [
        #     [i, input_lyr_defn.GetFieldDefn(i).GetNameRef()]
        #     for i in range(input_lyr_defn.GetFieldCount())
        # ]
        # # Delete the fields in reverse order
        # for field_i, field_name in input_fields[::-1]:
        #     if field_name == 'MGRS_ZONE':
        #         input_layer.DeleteField(field_i)
        # input_ds = None

        # # DEADBEEF - Fill missing OPENET_ID values in Montana
        # if state == 'MT':
        #     input_ds = shp_driver.Open(shp_path, 0)
        #     input_layer = input_ds.GetLayer()
        #     input_id_list = []
        #     for input_ftr in input_layer:
        #         openet_id = input_ftr.GetField('OPENET_ID')
        #         try:
        #             input_id_list.append(int(openet_id.split('_')[1]))
        #         except:
        #             pass
        #     input_ds = None
        #     # max_id = max(input_id_list)
        #     max_id = 899999
        #
        #     output_ds = shp_driver.Open(shp_path, 1)
        #     output_layer = output_ds.GetLayer()
        #     output_layer.SetAttributeFilter("OPENET_ID IS NULL")
        #     for output_ftr in output_layer:
        #         print(output_ftr.GetField('OPENET_ID'), output_ftr.GetField('MGRS_TILE'))
        #         max_id += 1
        #         output_ftr.SetField('OPENET_ID', f'MT_{max_id}')
        #         output_layer.SetFeature(output_ftr)
        #     output_ds = None


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Download and preprocess the state field shapefiles',
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
        years=args.years,
        overwrite_flag=args.overwrite,
        gee_key_file=args.key,
        project_id=args.project,
    )
