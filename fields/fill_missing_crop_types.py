import argparse
import logging
import os
import pprint

from osgeo import ogr
import pandas as pd

# import openet.core.utils as utils

ogr.UseExceptions()


def main(states, overwrite_flag=False):
    """Fill missing crop type values

    Parameters
    ----------
    states : list
    overwrite_flag : bool, optional

    """
    logging.info('\nFill missing crop type values')

    field_ws = os.getcwd()
    shapefile_ws = os.path.join(field_ws, 'shapefiles')

    remap_path = os.path.join(os.path.dirname(field_ws), 'cdl_annual_crop_remap_table.csv')

    cdl_annual_remap_years = []
    # CGM - Add any later years that may need to be filled
    # cdl_annual_remap_years.append([2024])
    year_min = 1997
    for year in list(range(2007, year_min-1, -1)):
        cdl_annual_remap_years.append([year+1, year])

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

    shp_driver = ogr.GetDriverByName('ESRI Shapefile')

    # Load the CDL annual crop remap
    # Values not in the remap table will stay the same
    remap_df = pd.read_csv(remap_path, comment='#')
    cdl_annual_remap = dict(zip(remap_df.IN, remap_df.OUT))
    # pprint.pprint(cdl_annual_remap)
    # input('ENTER')


    for state in states:
        logging.info(f'\n{state}')

        shp_path = os.path.join(shapefile_ws, state, f'{state}.shp')
        logging.debug(f'  {shp_path}')
        if not os.path.isfile(shp_path):
            logging.info('  State shapefile does not exist - skipping')
            continue

        # Clear any crop type/sources that were copied from another field
        if overwrite_flag:
            logging.info(f'Clearing existing filled values')
            output_ds = shp_driver.Open(shp_path, 1)
            output_layer = output_ds.GetLayer()
            for output_ftr in output_layer:
                for src_year, tgt_year in cdl_annual_remap_years:
                    crop_type = output_ftr.GetFieldAsInteger(f'CROP_{tgt_year}')
                    if crop_type == 0:
                        continue
                    crop_src = output_ftr.GetField(f'CSRC_{tgt_year}')
                    if crop_src.startswith(f'CROP_{src_year}'):
                        output_ftr.SetField(f'CROP_{tgt_year}', 0)
                        output_ftr.SetField(f'CSRC_{tgt_year}', '')
                output_layer.SetFeature(output_ftr)
            output_ds = None


        # Processing filling pre-2008 years in reverse order so that a year
        #   is always filled from the next (hopefully already filled) year.
        # This will take longer but will better fill gaps when a state is only
        #   missing a single year.
        # The commented out approach below would be better if we always used
        #   the remapped 2008 CDL image/values for all pre-2008 years.
        for src_year, tgt_year in cdl_annual_remap_years:
            logging.info(f'Copying CROP_{src_year} to CROP_{tgt_year}')
            logging.debug('  Remapping annual crops to code 47')
            output_ds = shp_driver.Open(shp_path, 1)
            output_layer = output_ds.GetLayer()
            for output_ftr in output_layer:
                # Get the crop type/source values for the source year
                crop_type = output_ftr.GetFieldAsInteger(f'CROP_{src_year}')
                crop_src = output_ftr.GetField(f'CSRC_{src_year}')
                # print(f'\n{src_year} {tgt_year}')
                # print(f'Input crop:      {crop_type}')
                # print(f'Input source:    {crop_src}')
                if crop_type == 0:
                    # logging.debug('  source crop_type not set - skipping')
                    continue

                # Get the crop type/source values for the target year
                tgt_crop_type = output_ftr.GetFieldAsInteger(f'CROP_{tgt_year}')
                # tgt_crop_src = output_ftr.GetField(f'CSRC_{tgt_year}')
                # print(f'Existing crop:   {tgt_crop_type}')
                if tgt_crop_type > 0 and not overwrite_flag:
                    # logging.debug('  target crop_type already set - skipping')
                    continue

                if crop_src.startswith('CROP_'):
                    # If the "new" crop source was built as a copy of a field,
                    #   pass that source to the target
                    tgt_crop_src = crop_src
                elif 'remapped annual crops' in crop_src:
                    # If the "old" crop source was remapped (but not a field copy),
                    #   add the remap note to the source
                    tgt_crop_src = f'CROP_{src_year} - remapped annual crops'
                else:
                    tgt_crop_src = f'CROP_{src_year}'
                # print(f'Output source:   {tgt_crop_src}')

                # If the source was already remapped it doesn't need it again
                # Convert the annual crops to the generic annual crop code
                if ('remapped annual crops' not in tgt_crop_src and
                      crop_type in cdl_annual_remap.keys()):
                    tgt_crop_type = cdl_annual_remap[crop_type]
                    tgt_crop_src += ' - remapped annual crops'
                    # print(f'Remapped crop:   {tgt_crop_type}')
                    # print(f'Remapped source: {tgt_crop_src}')
                else:
                    tgt_crop_type = crop_type
                    # print(f'Output crop:     {tgt_crop_type}')

                output_ftr.SetField(f'CROP_{tgt_year}', tgt_crop_type)
                output_ftr.SetField(f'CSRC_{tgt_year}', tgt_crop_src)
                output_layer.SetFeature(output_ftr)
            output_ds = None


        # # TODO: Add a flag to enable/disable annual crop remapping
        # logging.info(f'Copying crop types to other years')
        # logging.info('  Remapping most annual crops to code 47')
        # src_years = list(cdl_annual_remap_years.keys())
        # # src_years = sorted(list(set(cdl_annual_remap_years.values())))
        # output_ds = shp_driver.Open(shp_path, 1)
        # output_layer = output_ds.GetLayer()
        # for output_ftr in output_layer:
        #     for src_year in src_years:
        #         # Get the existing crop type/source values for the source year
        #         crop_type = output_ftr.GetFieldAsInteger(f'CROP_{src_year}')
        #         # crop_src = output_ftr.GetField(f'CSRC_{src_year}')
        #         if crop_type == 0:
        #             continue
        #         # Convert annual crops to the generic annual crop code
        #         try:
        #             crop_type = cdl_annual_remap[crop_type]
        #         except:
        #             pass
        #
        #         for tgt_year in cdl_annual_remap_years[src_year]:
        #             tgt_crop_type = output_ftr.GetFieldAsInteger(f'CROP_{tgt_year}')
        #             # tgt_crop_src = output_ftr.GetField(f'CSRC_{tgt_year}')
        #             if tgt_crop_type > 0 and not overwrite_flag:
        #                 continue
        #             output_ftr.SetField(f'CROP_{tgt_year}', crop_type)
        #             output_ftr.SetField(
        #                 f'CSRC_{tgt_year}', f'Remapped from CROP_{src_year}')
        #             # output_ftr.SetField(f'CSRC_{tgt_year}', crop_src)
        #     output_layer.SetFeature(output_ftr)
        # output_ds = None


def arg_parse():
    """"""
    parser = argparse.ArgumentParser(
        description='Fill missing crop type values',
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
