import src.utils as utils
from arcgis.gis import GIS
import src.assets as src_assets

target_itemid = src_assets.WATER_PROD_QC_DASHBOARD_BACKEND
gis = GIS('home') # update to user/pw 
item = gis.content.get(target_itemid)

utils.backup_water(verbose=True, test_run=True) # test_run=True copies csvs; test_run=False copies csvs and fgdb
utils.dashboard_etl(test_run=False, include_deletes=False, verbose=True) # regenerates dashboard backend file and writes csv
wqp = utils.wqp_wqx(test_run=False) # regenerates wqp dataset and metadata file, and writes csvs
utils.backup_veg(verbose=True)

# exports
dashb = utils.dashboard_etl(test_run=True) # returns a dataframe of un/verified records in dashboard format
wqp = utils.wqp_wqx(test_run=True) # returns a dataframe of verified records in wqp format
wqp_metadata = utils.wqp_metadata() # returns a dataframe of metadata to accompany wqp in R package NCRNWater
water_sites = utils.water_sites(r'output/ncrn_water_locations_20250122.csv') # returns a dataframe of current NCRN water sites and their attributes from AGOL
