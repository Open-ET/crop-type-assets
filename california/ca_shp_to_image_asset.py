import argparse
from datetime import datetime, timezone
import logging
import math
import os
# import pprint

import ee
from google.cloud import storage
import osgeo
from osgeo import gdal, ogr, osr
import pandas as pd

logging.getLogger('earthengine-api').setLevel(logging.INFO)
logging.getLogger('googleapiclient').setLevel(logging.INFO)
logging.getLogger('requests').setLevel(logging.INFO)
logging.getLogger('urllib3').setLevel(logging.INFO)

PROJECT_NAME = 'openet'
STORAGE_CLIENT = storage.Client(project=PROJECT_NAME)
BUCKET_NAME = 'openet_temp'
BUCKET_FOLDER = 'cadwr'
YEARS = []


def main(YEARS, overwrite_flag=False):
    """

    Parameters
    ----------
    years
    overwrite_flag : bool, optional

    Returns
    -------

    """
    workspace = os.getcwd()
    src_ws = os.path.join(workspace, 'sources')
    shp_ws = os.path.join(workspace, 'shapefiles')
    tif_ws = os.path.join(workspace, 'images')
    map_ws = os.path.join(workspace, 'remaps')

    if not YEARS:
        YEARS = [2014, 2016, 2018, 2019, 2020, 2021, 2022, 2023]

    # Hardcoding the shapefile folders and names for now since they are all slightly different
    src_paths = {
        2014: os.path.join(src_ws, 'i15_Crop_Mapping_2014_SHP', 'i15_Crop_Mapping_2014.shp'),
        2016: os.path.join(src_ws, 'i15_Crop_Mapping_2016_SHP', 'i15_Crop_Mapping_2016.shp'),
        2018: os.path.join(src_ws, 'i15_Crop_Mapping_2018_SHP', 'i15_Crop_Mapping_2018.shp'),
        2019: os.path.join(src_ws, 'i15_Crop_Mapping_2019', 'i15_Crop_Mapping_2019.shp'),
        2020: os.path.join(src_ws, 'i15_Crop_Mapping_2020', 'i15_Crop_Mapping_2020.shp'),
        2021: os.path.join(src_ws, 'i15_Crop_Mapping_2021_SHP', 'i15_Crop_Mapping_2021.shp'),
        2022: os.path.join(src_ws, 'i15_Crop_Mapping_2022_SHP', 'i15_Crop_Mapping_2022.shp'),
        2023: os.path.join(src_ws, 'i15_Crop_Mapping_2023_Provisional_SHP', 'i15_Crop_Mapping_2023_Provisional.shp'),
    }


    project_id = 'projects/openet/assets'

    collection_folder = f'{project_id}/crop_type/california'

    band_name = 'cropland'

    ee.Initialize()

    # if not ee.data.getInfo(collection_folder):
    #     logging.info('\nFolder does not exist and will be built'
    #                  '\n  {}'.format(collection_folder))
    #     input('Press ENTER to continue')
    #     ee.data.createAsset({'type': 'FOLDER'}, collection_folder)

    for year in YEARS:
        if year not in YEARS:
            raise ValueError(f'unsupported year {year}')
        logging.info(f'\n{year}')

        # Building projections from EPSG codes by year instead of reading from
        #   the shapefiles later on
        input_srs = osr.SpatialReference()
        if year in [2019, 2020, 2021, 2023]:
            input_srs.ImportFromEPSG(4269)
        elif year in [2022]:
            input_srs.ImportFromEPSG(3310)
        elif year in [2014, 2016, 2018]:
            input_srs.ImportFromEPSG(3857)
        else:
            raise ValueError(f'no default EPSG code for year {year}')
        # This conversion is needed to get the .Transform() call to work later on
        # TODO: Check if it is needed on the source and destination projections
        #   or just the source
        if int(osgeo.__version__[0]) >= 3:
            # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
            input_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
        # input_srs.MorphToESRI()
        # input_proj = input_srs.ExportToWkt()

        src_driver = ogr.GetDriverByName('ESRI Shapefile')
        # src_driver = ogr.GetDriverByName('OpenFileGDB')
        # src_driver = ogr.GetDriverByName('FileGDB')
        shp_driver = ogr.GetDriverByName('ESRI Shapefile')
        tif_driver = gdal.GetDriverByName('GTiff')
        # mem_driver = ogr.GetDriverByName('MEMORY')

        if not os.path.isdir(shp_ws):
            os.makedirs(shp_ws)
        if not os.path.isdir(tif_ws):
            os.makedirs(tif_ws)

        # Load the California to CDL remap (MB-Changed to read a single remap table for 2016-2023)
        if year == 2014:
            remap_df = pd.read_csv(
                os.path.join(map_ws, f'ca{year}_cdl_remap_table.csv'), comment='#'
            )
        else:
            remap_df = pd.read_csv(
                os.path.join(map_ws, f'ca2016_2023_cdl_remap_table.csv'), comment='#'
            )
        # remap_df = pd.read_csv(
        #     os.path.join(map_ws, f'ca{year}_cdl_remap_table.csv'), comment='#'
        # )
        ca_cdl_remap = dict(zip(remap_df.IN, remap_df.OUT))


        src_path = src_paths[year]
        shp_path = os.path.join(shp_ws, f'ca{year}_cdl.shp')
        logging.info(f'  {src_path}')
        logging.info(f'  {shp_path}')

        if overwrite_flag and os.path.isfile(shp_path):
            logging.info('  Shapefile already exists - removing')
            shp_driver.DeleteDataSource(shp_path)
        # elif not os.path.isfile(shp_path) and not os.path.isdir(src_path):
        #     logging.info('  Source does not exist - skipping')
        #     continue

        # For now assume the data can be read into memory
        if not os.path.isfile(shp_path):
            logging.info('Reading source features into memory')
            src_ds = src_driver.Open(src_path, 0)
            src_layer = src_ds.GetLayer()
            # src_srs = src_layer.GetSpatialRef()
            # if int(osgeo.__version__[0]) >= 3:
            #     # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
            #     src_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            # logging.debug(f'  Projection: {src_srs}')
            # input('ENTER')

            src_features = []
            for src_ftr in src_layer:
                src_fid = src_ftr.GetFID()
                geometry = src_ftr.GetGeometryRef().Clone()
                if year in [2019, 2020, 2021, 2022, 2023]:
                    crop_type = src_ftr.GetField(f'CROPTYP2')
                    main_crop = src_ftr.GetField(f'MAIN_CROP')
                elif year in [2016, 2018]:
                    crop_type = src_ftr.GetField(f'CROPTYP2')
                    main_crop = None
                else:
                    crop_type = src_ftr.GetField(f'Crop{year}')
                    main_crop = None

                try:
                    if main_crop and main_crop != '****':
                        cdl_code = ca_cdl_remap[main_crop]
                    else:
                        cdl_code = ca_cdl_remap[crop_type]
                except KeyError:
                    logging.info(f'  {src_fid} - Unexpected crop: {crop_type}')
                    input('ENTER')
                    continue
                src_features.append({'GEOM': geometry, 'CDL': cdl_code})

            # Close the dataset
            src_ds = None

            # This could be written to an in memory dataset also
            logging.info('Writing shapefile')
            shp_ds = shp_driver.CreateDataSource(shp_path)
            shp_layer = shp_ds.CreateLayer(
                f'ca{year}', input_srs, geom_type=ogr.wkbMultiPolygon
                # f'ca{year}', src_srs, geom_type=ogr.wkbMultiPolygon
            )
            shp_layer.CreateField(ogr.FieldDefn('CDL', ogr.OFTInteger))
            for src_ftr in src_features:
                shp_ftr = ogr.Feature(shp_layer.GetLayerDefn())
                shp_ftr.SetGeometry(src_ftr['GEOM'])
                shp_ftr.SetField('CDL', src_ftr['CDL'])
                shp_layer.CreateFeature(shp_ftr)
                shp_ftr = None
            # shp_layer.DeleteField(0)
            shp_ds = None

            # # The PRJ file isn't getting fully written or something
            # # Trying this approach from the cookbook
            # shp_prj = open(shp_path.replace('.shp', '.prj'), 'w')
            # shp_prj.write(input_proj)
            # shp_prj.close()


        # Create the raster in the California NAD83 Albers Projection
        logging.info(f'Rasterizing fields')
        epsg = 6414
        tif_path = os.path.join(tif_ws, f'ca{year}_cdl.tif')
        bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(tif_path)}'
        asset_id = f'{collection_folder}/{year}'
        logging.info(f'  GeoTIFF: {tif_path}')
        logging.info(f'  Bucket:  {bucket_path}')
        logging.info(f'  Asset:   {asset_id}')

        if os.path.isfile(tif_path) and overwrite_flag:
            logging.debug('  GeoTIFF already exists - removing')
            tif_driver.Delete(tif_path)
        if not os.path.isfile(tif_path):
            # Build the coordinate transformation object
            output_srs = osr.SpatialReference()
            output_srs.ImportFromEPSG(epsg)
            # TODO: Test if this is needed for the output projection
            if int(osgeo.__version__[0]) >= 3:
                # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
                output_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
            output_proj = output_srs.ExportToWkt()
            output_tx = osr.CoordinateTransformation(input_srs, output_srs)

            # Figure out the shape of the output raster
            logging.info('  Computing raster extent')
            shp_ds = shp_driver.Open(shp_path, 0)
            shp_layer = shp_ds.GetLayer()

            # Compute the convex hull of the fields instead of using the shp extent
            shp_geom_coll = ogr.Geometry(ogr.wkbGeometryCollection)
            for feature in shp_layer:
                ftr_geom = feature.GetGeometryRef().Clone()
                shp_geom_coll.AddGeometry(ftr_geom)
            shp_geom = shp_geom_coll.ConvexHull()

            # Project the convex hull and then get the bounding extent
            shp_geom.Transform(output_tx)
            shp_extent = shp_geom.GetEnvelope()

            # Convert the extent from OGR format to GDAL format
            output_extent = [shp_extent[0], shp_extent[2], shp_extent[1], shp_extent[3]]

            # Snap the extent to the output grid
            # Assume a 0, 0 grid corner for non-EPSG326XX projections
            snap_x, snap_y = 0, 0
            output_cs = 30
            output_extent = [
                math.floor((output_extent[0] - snap_x) / output_cs) * output_cs + snap_x,
                math.floor((output_extent[1] - snap_y) / output_cs) * output_cs + snap_y,
                math.ceil((output_extent[2] - snap_x) / output_cs) * output_cs + snap_x,
                math.ceil((output_extent[3] - snap_y) / output_cs) * output_cs + snap_y,
            ]
            output_cols = int((output_extent[2] - output_extent[0]) / output_cs)
            output_rows = int((output_extent[3] - output_extent[1]) / output_cs)
            output_geo = [output_extent[0], output_cs, 0., output_extent[3], 0., -output_cs]
            logging.info(f'    Extent: {output_extent}')
            logging.info(f'    Geo: {output_geo}')
            logging.info(f'    Cols: {output_cols}')
            logging.info(f'    Rows: {output_rows}')

            # Convert the shapefile to raster
            logging.info('  Writing GeoTIFF')
            output_ds = tif_driver.Create(
                tif_path, output_cols, output_rows, 1, gdal.GDT_Byte,
                ['COMPRESS=LZW', 'TILED=YES']
            )
            output_ds.SetProjection(output_proj)
            output_ds.SetGeoTransform(output_geo)
            output_band = output_ds.GetRasterBand(1)
            output_nodata = 0
            # output_nodata = 255
            output_band.SetNoDataValue(output_nodata)
            output_band.Fill(output_nodata)
            gdal.RasterizeLayer(output_ds, [1], shp_layer, options=['ATTRIBUTE=CDL'])

            # # Update the tile using the projected mask raster
            # output_band = output_ds.GetRasterBand(1)
            # output_array = output_band.ReadAsArray(0, 0, output_cols, output_rows)
            # output_array[mask_array != 1] = output_nodata
            # output_band.WriteArray(output_array, 0, 0)

            # Close the datasets
            shp_ds = None
            output_ds = None

        if os.path.isfile(tif_path):
            logging.info('  Uploading to bucket')
            bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
            blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(tif_path)}')
            blob.upload_from_filename(tif_path)

        # if overwrite_flag or not ee.date.getInfo(asset_id):
        logging.info('  Ingesting into Earth Engine')
        task_id = ee.data.newTaskId()[0]
        logging.debug(f'  {task_id}')
        params = {
            'name': asset_id,
            'bands': [{'id': band_name, 'tilesetId': 'image'}],
            'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
            'startTime': datetime(year, 1, 1).isoformat() + '.000000000Z',
            'pyramidingPolicy': 'MODE',
            'properties': {
                'date_updated': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            },
        }
        try:
            ee.data.startIngestion(task_id, params, allow_overwrite=True)
        except Exception as e:
            logging.exception(f'  Exception: {e}\n  Exiting')
            return False


        # Generate separate images for UTM zones 10 and 11
        for epsg in [32610, 32611]:
            utm_zone = int(str(epsg)[-2:])
            logging.info(f'Rasterizing fields for UTM zone {utm_zone}')

            tif_path = os.path.join(tif_ws, f'ca{year}_cdl_utm{utm_zone}.tif')
            bucket_path = f'gs://{BUCKET_NAME}/{BUCKET_FOLDER}/{os.path.basename(tif_path)}'
            asset_id = f'{collection_folder}/{year}_utm{utm_zone}'
            logging.info(f'  {tif_path}')
            logging.info(f'  {bucket_path}')
            logging.info(f'  {asset_id}')

            if overwrite_flag and os.path.isfile(tif_path):
                logging.debug('  GeoTIFF already exists - removing')
                tif_driver.Delete(tif_path)
            if not os.path.isfile(tif_path):
                # Build the coordinate transformation object
                output_srs = osr.SpatialReference()
                output_srs.ImportFromEPSG(epsg)
                output_proj = output_srs.ExportToWkt()
                output_tx = osr.CoordinateTransformation(input_srs, output_srs)

                # Figure out the shape of the raster
                logging.info('  Computing raster extent')
                shp_ds = shp_driver.Open(shp_path, 0)
                shp_layer = shp_ds.GetLayer()

                # Compute the convex hull of the fields instead of using the shp extent
                shp_geom_coll = ogr.Geometry(ogr.wkbGeometryCollection)
                for feature in shp_layer:
                    shp_geom_coll.AddGeometry(feature.GetGeometryRef().Clone())
                shp_geom = shp_geom_coll.ConvexHull()

                # Project the convex hull and then get the bounding extent
                shp_geom.Transform(output_tx)
                shp_extent = shp_geom.GetEnvelope()

                # Convert the extent from OGR format to GDAL format
                output_extent = [shp_extent[0], shp_extent[2], shp_extent[1], shp_extent[3]]

                # Snap the extent to the Landsat grid
                snap_x, snap_y = 15, 15
                output_cs = 30
                output_extent = [
                    math.floor((output_extent[0] - snap_x) / output_cs) * output_cs + snap_x,
                    math.floor((output_extent[1] - snap_y) / output_cs) * output_cs + snap_y,
                    math.ceil((output_extent[2] - snap_x) / output_cs) * output_cs + snap_x,
                    math.ceil((output_extent[3] - snap_y) / output_cs) * output_cs + snap_y,
                ]
                output_cols = int((output_extent[2] - output_extent[0]) / output_cs)
                output_rows = int((output_extent[3] - output_extent[1]) / output_cs)
                output_geo = [output_extent[0], output_cs, 0., output_extent[3], 0., -output_cs]
                logging.info(f'    Extent: {output_extent}')
                logging.info(f'    Geo: {output_geo}')
                logging.info(f'    Cols: {output_cols}')
                logging.info(f'    Rows: {output_rows}')

                # Convert the shapefile to raster
                logging.info('  Writing GeoTIFF')
                output_ds = tif_driver.Create(
                    tif_path, output_cols, output_rows, 1, gdal.GDT_Byte,
                    ['COMPRESS=LZW', 'TILED=YES']
                )
                output_ds.SetProjection(output_proj)
                output_ds.SetGeoTransform(output_geo)
                output_band = output_ds.GetRasterBand(1)
                output_nodata = 0
                # output_nodata = 255
                output_band.SetNoDataValue(output_nodata)
                output_band.Fill(output_nodata)
                gdal.RasterizeLayer(output_ds, [1], shp_layer, options=['ATTRIBUTE=CDL'])

                # Update the tile using the projected mask raster
                # output_band = output_ds.GetRasterBand(1)
                # output_array = output_band.ReadAsArray(0, 0, output_cols, output_rows)
                # output_array[mask_array != 1] = output_nodata
                # output_band.WriteArray(output_array, 0, 0)

                # Close the datasets
                shp_ds = None
                output_ds = None

            # if overwrite_flag or not ee.Date.getInfo(asset_id):

            if os.path.isfile(tif_path):
                logging.info('  Uploading to bucket')
                bucket = STORAGE_CLIENT.bucket(BUCKET_NAME)
                blob = bucket.blob(f'{BUCKET_FOLDER}/{os.path.basename(tif_path)}')
                blob.upload_from_filename(tif_path)

            logging.info('  Ingesting into Earth Engine')
            task_id = ee.data.newTaskId()[0]
            logging.debug(f'  {task_id}')
            params = {
                'name': asset_id,
                'bands': [{'id': band_name, 'tilesetId': 'image'}],
                'tilesets': [{'id': 'image', 'sources': [{'uris': [bucket_path]}]}],
                'startTime': datetime(year, 1, 1).isoformat() + '.000000000Z',
                'pyramidingPolicy': 'MODE',
                'properties': {
                    'date_updated': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                },
            }
            try:
                ee.data.startIngestion(task_id, params, allow_overwrite=True)
            except Exception as e:
                logging.exception(f'  Exception: {e}\n  Exiting')
                return False


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Convert California Crop Mapping shapefiles to GEE image assets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--years', nargs='+', type=int, choices=YEARS, help='Years to process')
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

    main(YEARS=args.years, overwrite_flag=args.overwrite)
