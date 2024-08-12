import src.utils as utils

if __name__ == 'main':
    # prod
    utils.backup_water(verbose=False, test_run=False)
    utils.backup_veg(verbose=False)
    utils.dashboard_etl(test_run=False, include_deletes=False, verbose=False) # prod
