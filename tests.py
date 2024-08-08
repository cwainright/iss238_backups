import src.assets as src_assets
import src.utils as utils
import src.water_backup as wtb
import datetime as dt

# tests

# test 1: query (get-request) each water table by name
df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_main'])
df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_ysi'])
df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_grabsample'])
df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_AGOL_ASSETS['tbl_photo'])

# test 2: make copies of each water table to a local folder
utils.backup_water(dest_dir='output', verbose=True)

# test 3: make copies of veg db files to a local folder
utils.backup_veg(src_dir=r'data', dest_dir='output')

# test 4: make copies of each water table to authoritative folder
utils.backup_water()

# test 5: make copies of each veg db file to authoritative folder
utils.backup_veg()

# test 6: save the csv-collection from agol
mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
wtb._agol_hosted_feature(newpath='output/', verbose=True, dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID, download_types=['CSV'])

# test 6: save the fgdb from agol
mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
wtb._agol_hosted_feature(newpath='output/', verbose=True, dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID, download_types=['File Geodatabase'])

# test 7: save both csv and fgdb from agol in one call
mytimestamp = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
wtb._agol_hosted_feature(newpath='output/', dir_ext=mytimestamp, in_fc=src_assets.WATER_DEV_ITEM_ID)
