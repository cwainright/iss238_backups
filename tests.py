import src.assets as src_assets
import src.utils as utils
import src.water_backup as wtb
import datetime as dt
import os

# tests

# test: query (get-request) each water table by name
# df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_main'])
# df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_ysi'])
# df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_grabsample'])
# df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_photo'])

# # test: make copies of each water table to a local folder
# utils.backup_water(dest_dir='output', verbose=True)

# # test: make copies of veg db files to a local folder
# utils.backup_veg(src_dir=r'data', dest_dir='output')

# # test: save the csv-collection from agol
# mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
# wtb._agol_hosted_feature(newpath='output/', verbose=True, dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID, download_types=['CSV'])

# # test: save the fgdb from agol
# mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
# wtb._agol_hosted_feature(newpath='output/', verbose=True, dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID, download_types=['File Geodatabase'])

# # test: save both csv and fgdb from agol in one call
# mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
# wtb._agol_hosted_feature(newpath='output/', verbose=True, dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID)

# # test: copy each survey from dev location
# dir_ext:str = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
# dest_dir:str='output'
# verbose=True
# newpath:str = os.path.join(dest_dir, dir_ext)
# src_dir=src_assets.SURVEY_DEV_DIRS
# for d in src_dir:
#     utils._backup_make_file_copies(dir_ext=dir_ext, newpath=newpath, src_dir=d, filetypes=['*'], verbose=verbose)

# # test: copy each survey from prod location
# dir_ext:str = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
# dest_dir:str='output'
# verbose=True
# newpath:str = os.path.join(dest_dir, dir_ext)
# src_dir=src_assets.SURVEY_SOURCE_DIRS
# for d in src_dir:
#     utils._backup_make_file_copies(dir_ext=dir_ext, newpath=newpath, src_dir=d, filetypes=['*'], verbose=verbose)

# # # test: a test-run -- save csv, fgdb, survey from dev asset
utils.backup_water(dest_dir='output', verbose=True, test_run=True)

# # test: a prod-run -- save csv, fgdb, survey from prod asset
# utils.backup_water(dest_dir='output', verbose=True, test_run=False)

# # test: make copies of each water table to authoritative folder
# utils.backup_water()

# # test: make copies of each veg db file to authoritative folder
# utils.backup_veg()

# test: make sure the extract step works properly
# myd = utils._extract(data_folder=src_assets.WATER_DEV_DATA_FPATH)
# myd = utils._extract(data_folder=src_assets.DM_WATER_BACKUP_FPATH)

# test: overwrite the dashboard backend feature layer, given a csv in the correct format
# from arcgis.features import FeatureLayerCollection
# from arcgis.gis import GIS
# import pandas as pd
# # make the test file
# myfile = os.path.join(src_assets.DM_WATER_BACKUP_FPATH, '2024-08-12_091545_949937')
# myfile = os.path.join(myfile, 'ncrn_discrete_water_dashboard_be_20240809.csv')
# df = pd.read_csv(myfile)
# df = df[[x for x in df.columns if 'week_of_year' not in x]].head()
# csv_filepath = r'data\ncrn_discrete_water_dashboard_be_20240715.csv'
# df.to_csv(csv_filepath,index=False)
# # overwrite the item with the testfile
# target_itemid = src_assets.WATER_DEV_QC_DASHBOARD_BACKEND
# gis = GIS('home') # update to user/pw 
# item = gis.content.get(target_itemid)
# dashboard_be = FeatureLayerCollection.fromitem(item)
# dashboard_be.properties.layers[0].name
# dashboard_be.manager.overwrite(csv_filepath)


# test: make the dashboard backend format
# df = utils.dashboard_etl(test_run=True) # dev
# df = utils.dashboard_etl(test_run=False) # prod


# prod mockup
utils.backup_water(verbose=True, test_run=False)
utils.dashboard_etl(test_run=False, include_deletes=False, verbose=True) # prod
utils.backup_veg(verbose=True)
