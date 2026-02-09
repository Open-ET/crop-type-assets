import argparse
import logging
import os
import pprint
import re

from osgeo import ogr
import pandas as pd

ogr.UseExceptions()


def main(states=[]):
    """Replace bad crop type values for specific areas and years

    Parameters
    ----------
    states : list

    """
    logging.info('\nReplace bad crop type values')

    field_ws = os.getcwd()
    shapefile_ws = os.path.join(field_ws, 'shapefiles')

    remap_path = os.path.join(os.path.dirname(field_ws), 'cdl_annual_crop_remap_table.csv')

    if states == ['ALL']:
        states = ['CO', 'NM', 'MX']
    else:
        states = sorted(list(set(y.strip() for x in states for y in x.split(',') if y.strip())))
    logging.info(f'States: {", ".join(states)}')

    shp_driver = ogr.GetDriverByName('ESRI Shapefile')

    # Load the CDL annual crop remap
    # Values not in the remap table will stay the same
    remap_df = pd.read_csv(remap_path, comment='#')
    cdl_annual_remap = dict(zip(remap_df.IN, remap_df.OUT))
    # pprint.pprint(cdl_annual_remap)
    # input('ENTER')

    if 'NM' in states:
        logging.info('\nReplace all 2008-2010 values in New Mexico HUC14 fields with the 2011 value')
        state = 'NM'
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.info(f'  {shp_path}')
        # if not os.path.isfile(shp_path):
        #     logging.info('  State shapefile does not exist - skipping')
        # with shp_driver.Open(shp_path, 1) as :
        output_ds = shp_driver.Open(shp_path, 1)
        output_layer = output_ds.GetLayer()
        # TODO: Test out filtering the layer instead of checking each feature HUC in the loop
        # output_layer.SetAttributeFilter("HUC12 LIKE '14%'")
        for output_ftr in output_layer:
            huc = str(output_ftr.GetField(f'HUC12'))
            if not huc.startswith('14'):
                continue
            src_year = 2011
            crop_type = output_ftr.GetFieldAsInteger(f'CROP_{src_year}')
            # crop_src = output_ftr.GetField(f'CSRC_{src_year}')
            if crop_type == 0:
                continue

            # The fill tool should set the values back to 1997,
            #   but applying here also in case this is run after the fill
            for tgt_year in range(2008, 2011):
                # TODO: Should these be set to remapped annual crop values?
                if crop_type in cdl_annual_remap.keys():
                    output_ftr.SetField(f'CROP_{tgt_year}', cdl_annual_remap[crop_type])
                    output_ftr.SetField(f'CSRC_{tgt_year}', f'CROP_{src_year} - remapped annual crops')
                else:
                    output_ftr.SetField(f'CROP_{tgt_year}', crop_type)
                    output_ftr.SetField(f'CSRC_{tgt_year}', f'CROP_{src_year}')
            output_layer.SetFeature(output_ftr)
        output_ds = None

    if 'CO' in states:
        logging.info('\nReplace all 2009 values in Colorado San Luis Valley HUCs '
                     '(130100 and 130201) with the 2008 value')
        state = 'CO'
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.info(f'  {shp_path}')
        output_ds = shp_driver.Open(shp_path, 1)
        output_layer = output_ds.GetLayer()
        # TODO: Test out filtering the layer instead of checking each feature HUC in the loop
        # output_layer.SetAttributeFilter("HUC12 LIKE '1301%'")
        for output_ftr in output_layer:
            huc = str(output_ftr.GetField(f'HUC12'))
            if not huc.startswith('130100') and not huc.startswith('130201'):
                continue
            tgt_year = 2009
            crop_type_before = output_ftr.GetFieldAsInteger(f'CROP_{tgt_year-1}')
            crop_type_after = output_ftr.GetFieldAsInteger(f'CROP_{tgt_year+1}')

            # The main goal for this is to avoid remapping if the before and after crops match
            # Default to using the crop type after the target year for all other cases
            if (crop_type_after > 0) and (crop_type_before == crop_type_after):
                # If the crop types before and after match, use that value directly
                #   instead of remapping to generic annual crop
                crop_type = crop_type_after
                crop_src = f'CROP_{tgt_year + 1}'
            elif crop_type_after in cdl_annual_remap.keys():
                # If the crop type after is a remapped annual, set the target year
                #   as a remapped annual and the source as the year after
                crop_type = cdl_annual_remap[crop_type_after]
                crop_src = f'CROP_{tgt_year + 1} - remapped annual crops'
            elif crop_type_after > 0:
                # Fallback on using the crop type after for all other fields
                crop_type = crop_type_after
                crop_src = f'CROP_{tgt_year + 1}'
            else:
                # This condition should only happen for very small polygons that should probably be removed
                logging.info(f'  {output_ftr.GetField(f"OPENET_ID")} - no crop type')
                continue

            output_ftr.SetField(f'CROP_{tgt_year}', crop_type)
            output_ftr.SetField(f'CSRC_{tgt_year}', crop_src)
            output_layer.SetFeature(output_ftr)
        output_ds = None

    if 'MX' in states:
        logging.info('\nSet all Mexico field values to 47')
        state = 'MX'
        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.info(f'  {shp_path}')
        output_ds = shp_driver.Open(shp_path, 1)
        output_layer = output_ds.GetLayer()
        for output_ftr in output_layer:
            for tgt_year in range(1997, 2024):
                output_ftr.SetField(f'CROP_{tgt_year}', 47)
                output_ftr.SetField(f'CSRC_{tgt_year}', 'DEFAULT')
            output_layer.SetFeature(output_ftr)
        output_ds = None


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Fill bad crop type values',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--states', default=['ALL'], nargs='+',
        help='Comma/space separated list of states')
    parser.add_argument(
        '--debug', default=logging.INFO, const=logging.DEBUG,
        help='Debug level logging', action='store_const', dest='loglevel')
    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = arg_parse()
    logging.basicConfig(level=args.loglevel, format='%(message)s')

    main(states=args.states)
