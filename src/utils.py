"""
Utilities for ETL of NCRN water data

These utilities download NCRN data from AGOL in relational format and then:
    a) back up source files for field and reviewer apps, the relational database, and update log files with backup results
    a) transform and load flattened datasets in NCRN-specific format (for internal applications like dashboards that need internal-only attributes)
    b) transform and load flattened datasets in wqp-wqx distribution format (for internal and external analysis-ready datasets)

main.py is the main API for these utilities

Initial release: 2024-11-06
Author: CAW

Requirements and caveats:
To use these the src.utils module, you need an AGOL token and you need read/write permission for every hosted-feature the module calls.
Review the assets.py module for all of the relevant hosted-features.
The easiest way to earn an AGOL token is to open ArcGIS Pro, and log in to your account.
A token is only valid for one session (i.e., while your computer is on), so you will need to renew your token after every computer restart.

Examples:
    import src.utils as utils
    from arcgis.gis import GIS
    import src.assets as src_assets

    # before running the backup routine, confirm that you have a valid AGOL token
    target_itemid = src_assets.WATER_PROD_QC_DASHBOARD_BACKEND
    gis = GIS('home') # 'home' uses your active token (i.e., the token you generated from ArcGIS Pro). GIS(). If this fails, you do not have a valid token .
    item = gis.content.get(target_itemid) # if this does not return a valid item ID, you do not have a valid token

    # run the backup routine
    utils.backup_water(verbose=True) # query records from prod
    utils.dashboard_etl(test_run=False) # transform and load to prod, does not return df
    df = utils.dashboard_etl(test_run=True) # returns df, does not load prod
    utils.wqp_wqx(test_run=False) # transform and load to prod, does not return df
    df = utils.wqp_wqx(test_run=True) # returns df, does not load prod

"""

import shutil
import os
import src.assets as assets
import src.water_backup as wtb
import src.transform as tf
import datetime as dt
import pandas as pd
import time
import numpy as np

def backup_water(dest_dir:str=assets.DM_WATER_BACKUP_FPATH, verbose:bool=False, test_run:bool=True) -> None:
    """Generic to make backups of NCRN water source files

    Args:
        dest_dir (str, optional): Absolute or relative filepath to receive the backup files. Defaults to assets.DM_WATER_BACKUP_FPATH.
        verbose (bool, optional): True turns on interactive messaging. Defaults to False.
        test_run (bool, optional): True makes a backup of source files and csvs (about 25 sec); False makes a backup of source files, csvs, and filegeodatabase (about 8 min). Defaults to True.

    Returns:
        None

    Examples:
        import src.utils as utils
        utils.backup_water(verbose=True, test_run=False)

    """
    start_time = time.time()
    assert os.path.exists(dest_dir)==True, print(f'You provided {dest_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')

    # make new directory to receive backup files
    dir_ext:str = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
    newpath:str = os.path.join(dest_dir, dir_ext)
    

    # download a csv of each table, and save each csv (for from-source-data restoration and/or input for ETL)
    # wtb._download_csvs(newpath=newpath, verbose=verbose, dir_ext=dir_ext)

    # copy the survey source-files
    dirs=assets.SURVEY_SOURCE_DIRS
    for d in dirs:
        try:
            # _backup_make_file_copies(src_dir=d, dest_dir=dest_dir, filetypes=['*'], verbose=verbose)
            _backup_make_file_copies(dir_ext=dir_ext, newpath=newpath, src_dir=d, filetypes=['*'], verbose=verbose)
        except:
            _add_log_entry(log_timestamp=dir_ext, src_file=d, log_dest=newpath, log_result='fail - unable to copy file')
            if verbose == True:
                print(f'Unable to copy directory {d=}. Files not backed up.')

    # download a copy of the hosted feature (for 1:1 restoration)
    if test_run==True:
        wtb._agol_hosted_feature(newpath=newpath, in_fc=assets.WATER_AGOL_ITEM_ID, verbose=verbose, dir_ext=dir_ext, download_types=['CSV'])
    else:
        wtb._agol_hosted_feature(newpath=newpath, in_fc=assets.WATER_AGOL_ITEM_ID, verbose=verbose, dir_ext=dir_ext, download_types=['CSV','File Geodatabase'])
    
    # TODO: take the latest verified records, replace their counterparts in the wqx for dan et al
    # wqp_wqx()
    # dashboard_etl()

    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]

    if verbose == True:
        print(f'`backup_water()` completed. Elapsed time: {elapsed_time}')

    return None

def backup_veg(src_dir:str=assets.VEG_T_DRIVE_FPATH, dest_dir:str=assets.DM_VEG_BACKUP_FPATH, filetypes:list=['.accdb'], verbose:bool=False) -> None:
    """Generic to make backups of NCRN forest vegetation source files

    Args:
        src_dir (str, optional): Relative or absolute filepath for the directory containing your source files. Defaults to assets.VEG_T_DRIVE_FPATH.
        dest_dir (str, optional): Relative or absolute filepath for the directory where you want to save your backup files. Defaults to 'output'.
        filetypes (list, optional): A list of filetypes you want to copy from `src_dir`. Defaults to ['.accdb'].
        verbose (bool, optional): True turns on feedback for interactive session. Defaults to False.

    Returns:
        None

    Examples:
        import src.utils as utils

        # example 1, copy a local db (data/*.accdb) to a local backup directory (output/)

        utils.backup_veg(src_dir='data', dest_dir='output')

        # example 2, copy the T-drive (authoritative as of 2024-08-01) db to a network backup directory

        utils.backup_veg()

    """
    start_time = time.time()
    # check that the source directory and file exist
    assert os.path.exists(src_dir)==True, print(f'You provided {src_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert os.path.exists(dest_dir)==True, print(f'You provided {dest_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert len(filetypes)>0, print(f'You provided {filetypes=}, which is an empty list. Provide one or more file extensions to copy from `src_dir` to `dest_dir`.')

    # extend `dest_dir` with a timestamp and check that that directory does not exist
    dir_ext:str = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
    newpath:str = _make_new_backup_dir(dest_dir=dest_dir, verbose=verbose, dir_ext=dir_ext)
    _backup_make_file_copies(dir_ext=dir_ext, newpath=newpath, src_dir=src_dir, filetypes=filetypes, verbose=verbose)

    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time = str(dt.timedelta(seconds=elapsed_time))
    elapsed_time = elapsed_time.split('.')[0]

    if verbose == True:
        print(f'`backup_veg()` completed. Elapsed time: {elapsed_time}')

    return None

def _add_log_entry(log_timestamp:str, src_file:str, log_dest:str, log_result:str, log_fpath:str=assets.DM_BACKUP_LOG_FPATH) -> None:
    """Make an entry in the job-log for each file

    Args:
        log_timestamp (str): _description_
        src_file (str): _description_
        log_dest (str): _description_
        log_result (str): _description_
        log_fpath (str, optional): _description_. Defaults to assets.DM_BACKUP_LOG_FPATH.

    Returns:
        _type_: _description_
    """
    # make a dataframe with one row to append to the log file
    log = pd.DataFrame(columns=['userid','log_timestamp','src_file','log_dest','log_result','log_fpath'])
    log['userid'] = [os.environ.get('USERNAME')]
    log['log_timestamp'] = [log_timestamp]
    log['log_dest'] = [log_dest]
    log['log_result'] = [log_result]
    log['log_fpath'] = [log_fpath]
    log['src_file'] = [src_file]

    # if log doesn't exist, create it
    if os.path.exists(log_fpath):
        existing_log = pd.read_csv(log_fpath)
        log = pd.concat([existing_log, log])
    log.to_csv(log_fpath, index=False)

    return None

def _make_new_backup_dir(dest_dir:str, verbose:bool, dir_ext:str) -> str:
    """Make a new directory inside an existing directory

    Args:
        dest_dir (str): An absolute or relative filepath to a directory; the directory into which you want to create the timestamp `dir_ext` directory.
        verbose (bool): Turns on or off success/failure messaging
        dir_ext (str): The name of the directory to create (usually a string timestamp like '2024-08-07_111830_376335')

    Returns:
        str: The absolute filepath to the directory created by this function
    """

    # if dir exists, fail
    newpath = os.path.join(dest_dir, dir_ext)
    # assert os.path.exists(newpath) == False, print(f'Your target directory {newpath=} already exists and this function is not allowed to overwrite existing data. The newpath is a timestamp so you can just try call the function again.')
    # if it does not exist, make dir
    if os.path.exists(newpath)==False:
        os.makedirs(newpath)
        if verbose == True:
            print(f'made dir: {newpath=}')

    return newpath

def _backup_make_file_copies(dir_ext:str, newpath:str, src_dir:str, filetypes:list, verbose:bool) -> None:

    # copy the source file(s) from the source directory to the target directory
    filetypes = tuple(set(filetypes))
    source_files = []
    if filetypes == ('*',):
        shutil.copytree(src_dir, newpath)
        log_res = 'success'
        for d in os.listdir(src_dir):
            _add_log_entry(log_timestamp=dir_ext, src_file=src_dir, log_dest=os.path.join(newpath, d), log_result=log_res)
            if verbose == True:
                print(f'copied {d} to {newpath}')
    else:
        for file in os.listdir(src_dir):
            if file.endswith(filetypes):
                # print(os.path.join(src_dir, file))
                source_files.append(os.path.join(src_dir, file))
        # print(source_files)
        if len(source_files) > 0:
            for f in source_files:
                try:
                    shutil.copy2(f, newpath)
                    log_res = 'success'
                    _add_log_entry(log_timestamp=dir_ext, src_file=f, log_dest=os.path.join(newpath, f.rsplit('\\',1)[1]), log_result=log_res)
                    if verbose == True:
                        print(f'copied {f} to {newpath}')
                except Exception as e:
                    print(e)
                    log_res = 'fail'
                    _add_log_entry(log_timestamp=dir_ext, src_file=f, log_dest=os.path.join(newpath, f.rsplit('\\',1)[1]), log_result=log_res)
        else:
            if verbose == True:
                print(f'No files of type {filetypes=} found in {src_dir=}. No files backed up.')
            _add_log_entry(log_timestamp=dir_ext, src_file=None, log_dest=newpath, log_result='no_files')

    return None

def dashboard_etl(test_run:bool=False, include_deletes:bool=False, verbose:bool=True) -> pd.DataFrame:
    """Extract-transform-load pipeline that transforms relational NCRN discrete water data into one flat csv and optionally overwrites the feature service underlying the dashboard with the csv

    This pipeline finds the newest version of the data in a folder, extracts the newest data, transforms the data into the format for the backend of the QC dashboard.
    If `load` == True, the pipeline will overwrite the data in the feature service.
    If `test_run` == True, the pipeline extracts data from a development asset; otherwise, the pipeline extracts data from the production asset.
    `include_deletes` applies cascade-deletes based on user-side fields (e.g., tbl_main.delete_record or tbl_ysi.delete_increment)

    Args:
        dest_dir (str, optional): relative or absolute filepath to a folder where you want to save the output file. Defaults to ''. If blank, will not write.
        test_run (bool, optional): False returns bool (True when all steps were successful) and overwrites the dashboard backend in AGOL. True returns a dataframe and does not overwrite development assets feature service. Defaults to False.
        include_deletes (bool, optional): a flag to include the soft-deleted records. True includes soft-deleted records. False filters-out soft-deleted records.

    Returns:
        If `test_Run` == True:
            pd.DataFrame: flattened dataframe of ncrn water results, flattened and melted to long format for formatting
        else:
            bool: True when all ETL steps were successful; False when one or more steps failed

    Examples:
        import src.utils as utils
        utils.dashboard_etl(test_run=False, include_deletes=False, verbose=True)
    """
    start_time = time.time()

    # Extract steps
    newest_data_folder:str = _find_newest_folder(assets.DM_WATER_BACKUP_FPATH) # find the newest timestamp folder
    df_dict:dict = _extract(newest_data_folder) # extract csvs to dataframes
    
    # Transform steps
    df:pd.DataFrame = tf._transform(df_dict=df_dict, include_deletes=include_deletes)
    dashboard_cols = pd.read_csv(r'data\ncrn_discrete_water_dashboard_be_20240822.csv', nrows=5).columns
    
    # QC checks
    if verbose == True:
        tf._quality_control(df)
    
    df = df[dashboard_cols]

    if test_run == True:
        print('df returned')
        if verbose == True:
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time = str(dt.timedelta(seconds=elapsed_time))
            elapsed_time = elapsed_time.split('.')[0]
            print(f'`dashboard_etl()` completed. Elapsed time: {elapsed_time}')
        return df
    else:
        # TODO:
        fname = wtb._save_dashboard_csv(df, newest_data_folder, verbose)
        wtb._load_feature(fname, assets.WATER_PROD_QC_DASHBOARD_BACKEND, verbose)
        if verbose == True:
            end_time = time.time()
            elapsed_time = end_time - start_time
            elapsed_time = str(dt.timedelta(seconds=elapsed_time))
            elapsed_time = elapsed_time.split('.')[0]
            print(f'`dashboard_etl()` completed. Elapsed time: {elapsed_time}')
        return True

def wqp_metadata(df:str='data/wqp.csv', write:str='') -> pd.DataFrame:
    """Build a NCRNWater-compatible metadata dataframe from a csv of WQP-formatted NCRN water records

    Args:
        df (str, optional): absolute or relative filepath to a csv of wqp-formatted NCRN water records; the output of utils.wqp(). Defaults to 'data/wqp.csv', which was updated on 2024-01-22.
        write (str, optional): absolute or relative filepath where you want to save a csv of the metadata dataframe. Only writes if not blank. Defaults to ''.

    Returns:
        pd.DataFrame: A dataframe containing NCRNWater-compatible metadata

    Examples:
        wqp_metadata = utils.wqp_metadata()
        wqp_metadata = utils.wqp_metadata(df='data/wqp.csv')
        wqp_metadata = utils.wqp_metadata(df='data/wqp.csv', write='output/ncrnwater_wqp_metadata.csv')
    """

    if write != '':
        assert write.endswith('.csv')

    newest_data_folder:str = _find_newest_folder(assets.DM_WATER_BACKUP_FPATH) # find the newest timestamp folder
    df_filepath = os.path.join(newest_data_folder, 'wqp.csv')

    # 1. read csvs
    md = pd.read_csv(r'data/MetaData.csv', encoding = "ISO-8859-1")
    df = pd.read_csv(df_filepath)
    df = df[df['CharacteristicName']!= 'Chlorine']
    md['SiteCodeWQX'] = md['SiteCode'] # preprocess; NCRN no longer maintains two site-naming-conventions

    # 2.a. Fix incongruencies: Site names
    md = _wqp_metadata_site_incongruency(df, md)
    # 2.b. Fix incongruencies: CharacteristicNames
    md = _wqp_metadata_char_incongruency(df, md)

    md = _wqp_metadata_qc(df, md)

    if write != '':
        md.to_csv(write, index=False)
        print(f"\nWrote WQP Metadata to: {write}\n")
    else:
        print("WQP Metadata dataframe returned")
    return md

def _wqp_metadata_char_incongruency(df:pd.DataFrame, md:pd.DataFrame) -> pd.DataFrame:

    # Incongruencies can go in either of two directions: adds or deletes
    
    # In this case, a "delete" can be a true delete, or simply an alias that needs updating
    updates = { # old name : new name
        'Flow':'Base flow discharge'
        ,'Algae, substrate rock/bank cover (choice list)':'Substrate algae, % (choice list)'
        ,'Depth':'Cross-Section Depth'
        ,'Gran acid neutralizing capacity':'Acid Neutralizing Capacity (ANC)'
        ,'RBP Stream Velocity': 'Velocity - stream'
        ,'Stream width measure':'Wetted Width'
        # ,'Stream physical appearance (choice list)':'Water appearance (text)' # this one isn't a simple de-alias, so we'll just delete and then add the corrected-version below
        ,'Flow, severity (choice list)':'Stream flow (choice list)'
    }
    for k,v in updates.items():
        mask = (md['DataName']==k)
        md['DataName'] = np.where(mask, v, md['DataName'])
    
    deletes = [x for x in md.DataName.unique() if x not in df.CharacteristicName.unique() and x not in updates.keys()] # present in md but absent from df; delete or update
    if len(deletes) > 0:
        mask = (md['DataName'].isin(deletes)==False)
        md = md[mask].reset_index(drop=True)

    # adds
    adds = [x for x in df.CharacteristicName.unique() if x not in md.DataName.unique()] # present in df but absent from md
    sites = md.SiteCodeWQX.unique()

    start_cols = md.columns
    # I think it'd be easiest to take one row per site from md

    # then blank-out all of the fields we need to update for each add
    blanks = [
        'CharacteristicName'
        ,'DisplayName'
        ,'DataName'
        ,'Category'
        ,'CategoryDisplay'
        ,'Units'
        ,'LowerPoint'
        ,'UpperPoint'
        ,'DataType'
        ,'LowerDescription'
        ,'UpperDescription'
        ,'AssessmentDetails'
    ]
    
    # Then populate the blanks from wqp

    # since only one field (units) can even come from the wqp, just make a lookup
    # this lookup has to be maintened through time
    lu = {}
    for a in adds:
        tmp = {x:None for x in blanks}
        tmp['DataName'] = a
        lu[a] = tmp

    # hard-code the lookup values
    x = 'Total Nitrogen, mixed forms'
    lu[x]['CharacteristicName'] = 'TotalN'
    lu[x]['DisplayName'] = 'Total Nitrogen'
    lu[x]['Category'] = 'TotalN'
    lu[x]['CategoryDisplay'] = 'Total Nitrogen'
    lu[x]['Units'] = df[df['CharacteristicName']==x]['ResultMeasure/MeasureUnitCode'].unique()[0]
    lu[x]['DataType'] = 'numeric'
    
    x ='Total Dissolved Nitrogen, mixed forms'
    lu[x]['CharacteristicName'] = 'TotalDN'
    lu[x]['DisplayName'] = 'Total Dissolved Nitrogen'
    lu[x]['Category'] = 'TotalDN'
    lu[x]['CategoryDisplay'] = 'Total Dissolved Nitrogen'
    lu[x]['Units'] = df[df['CharacteristicName']==x]['ResultMeasure/MeasureUnitCode'].unique()[0]
    lu[x]['DataType'] = 'numeric'

    x ='Total Dissolved Phosphorus, mixed forms'
    lu[x]['CharacteristicName'] = 'TotalDP'
    lu[x]['DisplayName'] = 'Total Dissolved Phosphorus'
    lu[x]['Category'] = 'TotalDP'
    lu[x]['CategoryDisplay'] = 'Total Dissolved Phosphorus'
    lu[x]['Units'] = df[df['CharacteristicName']==x]['ResultMeasure/MeasureUnitCode'].unique()[0]
    lu[x]['DataType'] = 'numeric'

    # x ='Chlorine'
    # lu[x]['CharacteristicName'] = 'Chlorine'
    # lu[x]['DisplayName'] = 'Chlorine'
    # lu[x]['Category'] = 'Chlorine'
    # lu[x]['CategoryDisplay'] = 'Chlorine'
    # lu[x]['Units'] = df[df['CharacteristicName']==x]['ResultMeasure/MeasureUnitCode'].unique()[0]
    # lu[x]['DataType'] = 'numeric'

    x ='Turbidity'
    lu[x]['CharacteristicName'] = 'Turbidity'
    lu[x]['DisplayName'] = 'Turbidity'
    lu[x]['Category'] = 'Turbidity'
    lu[x]['CategoryDisplay'] = 'Turbidity'
    lu[x]['Units'] = df[df['CharacteristicName']==x]['ResultMeasure/MeasureUnitCode'].unique()[0]
    lu[x]['DataType'] = 'numeric'

    x ='Substrate algae color'
    lu[x]['CharacteristicName'] = 'AlgaeColor'
    lu[x]['DisplayName'] = 'Algae Color'
    lu[x]['Category'] = 'AlgaeColor'
    lu[x]['CategoryDisplay'] = 'Algae Color'
    lu[x]['Units'] = None
    lu[x]['DataType'] = 'factor'

    x ='Water appearance (text)'
    lu[x]['CharacteristicName'] = 'WaterAppearance'
    lu[x]['DisplayName'] = 'Water Appearance'
    lu[x]['Category'] = 'WaterAppearance'
    lu[x]['CategoryDisplay'] = 'Water Appearance'
    lu[x]['Units'] = None
    lu[x]['DataType'] = 'factor'

    # then add those rows back to md
    # and repeat that process for every add
    for k,v in lu.items():
        for b in blanks:
            template = md[md['DataName']==md.DataName.unique()[0]].copy().reset_index(drop=True) # make a "template" 1-row dataframe with the correct columns
            mask = (template[b].isna()==False)
            template[b] = np.where(mask, None, template[b]) # blank-out all of the columns that are present in the lookup `lu`
        for x,y in v.items():
            template[x] = y # now fill-in the blanks from `lu`
        # print(template.to_string())
        md = pd.concat([md, template]).reset_index(drop=True).sort_values(['SiteCodeWQX','Category']) # and append the filled-in template to `md`

    end_cols = md.columns
    if len(start_cols) != len(end_cols): # sanity check; we should not be adding or deleting any columns
        print(f'WARNING: _wqp_metadata_char_incongruency() added {len(end_cols)-len(start_cols)} columns:')
        if len(start_cols) > len(end_cols):
            print([x for x in start_cols if x not in end_cols])
        else:
            print([x for x in end_cols if x not in start_cols])

    return md

def _wqp_metadata_qc_repair_missing_sitechar_combinations(md:pd.DataFrame) -> pd.DataFrame:

    before_len = len(md)

    # for some reason ANCR has all of the characteristics, so we compare other sites to it
    ancr = md[md['SiteCode']=='NCRN_ANTI_ANCR'].CharacteristicName.unique()
    assert len(ancr == 28) # if this is not true, the data model changed and the program should fail until the dev fixes

    # Determine which sites are missing which CharacteristicNames, relative to NCRN_ANTI_ANCR
    sites_missing = {}
    for site in md.SiteCode.unique():
        mask = (md['SiteCode']==site)
        subset = md[mask]
        chars = subset.CharacteristicName.unique()
        if len(chars) == len(ancr):
            pass
        elif len(chars) > len(ancr):
            # if this happens, dev must re-think how to handle
            print(f"WARNING: {site} has {len(chars)} characteristics and that's more than expected. Review the metadata file!")
        elif len(chars) < len(ancr):
            missing_chars = [x for x in ancr if x not in chars]
            sites_missing[site] = missing_chars

    # count the number of rows we should be adding to the final dataframe, so we can validate the output
    counter = 0
    for k,v in sites_missing.items():
        counter += len(v)

    # find 'neighbors': another site in that park that has the rows we need
    # a neighbor is a site in the same park that we can borrow a metadata entry from
    for k,v in sites_missing.items():
        # figure out what park to check
        park = k.split('_')[1]
        mask = (md['SiteCode']==k)
        # collect the location attributes that we need to keep
        ParkCode = md[mask].ParkCode.unique()[0]
        ShortName = md[mask].ShortName.unique()[0]
        LongName = md[mask].LongName.unique()[0]
        SiteCode = md[mask].SiteCode.unique()[0]
        SiteCodeWQX = md[mask].SiteCodeWQX.unique()[0]
        SiteName = md[mask].SiteName.unique()[0]
        Lat = md[mask].Lat.unique()[0]
        Long = md[mask].Long.unique()[0]
        for char in v:
            mask = (md['SiteCode'].str.contains(park)) & (md['CharacteristicName']==char) # neighbor
            neighbors = md[mask]

            # program should fail if there are no neighbors, dev must address
            assert len(neighbors) >0, print(f'FAIL: {k} has no neighbors with the characteristic {char}')

            mask = (neighbors['SiteCode']==max(neighbors['SiteCode']))
            newrow = neighbors[mask].copy()
            newrow['ParkCode']=ParkCode
            newrow['ShortName']=ShortName
            newrow['LongName']=LongName
            newrow['SiteCode']=SiteCode
            newrow['SiteCodeWQX']=SiteCodeWQX
            newrow['SiteName']=SiteName
            newrow['Lat']=Lat
            newrow['Long']=Long
            md = pd.concat([md,newrow])
    
    after_len = len(md)
    
    assert (after_len == before_len + counter), print(f"FAIL: _wqp_qc_repair_missing_sitechar_combinations() failed to reconcile broken site/char combinations")

    return md

def _wqp_metadata_qc_greenbelt(md:pd.DataFrame) -> pd.DataFrame:

    mask = (md['ParkCode']=='GREE')
    cols = ['ShortName','LongName','ParkCode'] # column-order matters here; must do ParkCode last
    for col in cols:
        tmp = None
        tmps = md[md['ParkCode']=='NACE'][col].unique()
        if len(tmps) == 1:
            tmp = tmps[0]
        else:
            problems+=1
            print(f'WARNING: failed to assign {col} for Greenbelt')
            print(tmps)
        md[col] = np.where(mask, tmp, md[col])

    return md

def _wqp_metadata_qc_spot_fixes(md:pd.DataFrame) -> pd.DataFrame:

    # replace mg/l and ueq/l (lower-case 'L') with mg/L and ueq/L (capital 'L')
    mask = (md['Units'].str.contains('/l'))
    md['Units'] = np.where(mask, md['Units'].str.replace('/l', '/L'), md['Units'])

    mask = (md['SiteCode']=='NCRN_PRWI_NFQC')
    md['SiteName'] = np.where(mask, 'North Fork Quantico Creek', md['SiteName']) # was erroneously labelled 'Quantico Creek'

    mask = (md['SiteCode']=='NCRN_PRWI_MBBR')
    md['SiteName'] = np.where(mask, 'Mary Byrd Branch', md['SiteName'])

    typos = {
        'Air Temperture':'Air Temperature'
        ,'Dishcarge':'Discharge'
    }
    for k,v in typos.items():
        mask = (md['DisplayName']==k)
        md['DisplayName'] = np.where(mask, v, md['DisplayName'])

    return md

def _wqp_metadata_qc_check_sitechar_combinations(md:pd.DataFrame, df:pd.DataFrame) -> int:
    """Check that every combination of site and characteristic in df exists in metadata

    Args:
        md (pd.DataFrame): NCRNWater-formatted metadata file
        df (pd.DataFrame): wqp-formatted data file

    Returns:
        int: the count of missing characteristic/site combinations
    """
    # find all combinations of Characteristic in df
    assert len(df[df['MonitoringLocationIdentifier'].isna() | df['CharacteristicName'].isna()]) == 0
    df2 = df[['MonitoringLocationIdentifier','CharacteristicName']].drop_duplicates(['MonitoringLocationIdentifier','CharacteristicName'])
    df2['key'] = df2['MonitoringLocationIdentifier'] + df2['CharacteristicName']

    # find all combinations of Characteristic in md
    assert len(md[md['SiteCode'].isna() | md['DataName'].isna()]) == 0
    md2 = md[['SiteCode', 'DataName']].drop_duplicates(['SiteCode','DataName'])
    md2['key'] = md2['SiteCode'] + md2['DataName']

    # does every combination in df exist in md?
    missing = [x for x in df2.key.unique() if x not in md2.key.unique()]
    if len(missing) >0:
        print(f"WARNING: there are {len(missing)} missing characteristic and site combinations in the metadata file!")
        for x in missing:
            print(x)

    return len(missing)

def _wqp_metadata_qc_check_charunit_combinations(md:pd.DataFrame, df:pd.DataFrame) -> int:
    """Check that every combination of characteristic and unit in df exists in metadata

    Args:
        md (pd.DataFrame): NCRNWater-formatted metadata file
        df (pd.DataFrame): wqp-formatted data file

    Returns:
        int: the count of missing characteristic/unit combinations
    """
    # find all combinations of Characteristic in df
    assert len(df[df['MonitoringLocationIdentifier'].isna() | df['CharacteristicName'].isna()]) == 0
    df2 = df[['MonitoringLocationIdentifier','CharacteristicName']].drop_duplicates(['MonitoringLocationIdentifier','CharacteristicName'])
    df2['key'] = df2['MonitoringLocationIdentifier'] + df2['CharacteristicName']

    # find all combinations of Characteristic in md
    assert len(md[md['SiteCode'].isna() | md['DataName'].isna()]) == 0
    md2 = md[['SiteCode', 'DataName']].drop_duplicates(['SiteCode','DataName'])
    md2['key'] = md2['SiteCode'] + md2['DataName']

    # does every combination in df exist in md?
    missing = [x for x in df2.key.unique() if x not in md2.key.unique()]
    if len(missing) >0:
        print(f"WARNING: there are {len(missing)} missing characteristic and site combinations in the metadata file!")
        for x in missing:
            print(x)

    return len(missing)

def _wqp_metadata_qc(df:pd.DataFrame, md:pd.DataFrame) -> None:
    problems = 0
    
    # are any combinations of site and characteristics missing?
    md = _wqp_metadata_qc_repair_missing_sitechar_combinations(md)
    
    # Replace greenbelt with NACE
    md = _wqp_metadata_qc_greenbelt(md)


    # check for prohibited nulls
    non_nullables = [
        'Network'
        ,'ParkCode'
        ,'ShortName'
        ,'LongName'
        ,'SiteCode'
        ,'SiteCodeWQX'
        ,'SiteName'
        ,'Lat'
        ,'Long'
        ,'Type'
        ,'CharacteristicName'
        ,'DisplayName'
        ,'DataName'
        ,'Category'
        ,'CategoryDisplay'
        ,'Units'
        ,'DataType'
    ]
    for col in non_nullables:
        mask = (md[col].isna()) & (md['DataType']!='factor')
        sub = md[mask]
        if len(sub) > 0:
            print(f'WARNING: non-nullable field {col} was null in {len(sub)} rows of wqp metadata.')
            problems += 1

    conditionally_nullable = {
        'LowerDescription': 'AssessmentDetails'
        ,'UpperDescription': 'AssessmentDetails'
    }
    for k,v in conditionally_nullable.items():
        mask = (md[k].isna()==False) & (md[v].isna())
        sub = md[mask]
        if len(sub) > 0:
            print(f'WARNING: conditinally-nullable field {v} was null in {len(sub)} rows of wqp metadata.')
            problems += 1

    # check for incongruencies
    mismatches = [x for x in md.DataName.unique() if x not in df.CharacteristicName.unique()]
    if len(mismatches) > 0:
        problems += len(mismatches)
        print(f'Metadata characteristics not present in dataframe:')
        print(mismatches)
    mismatches = [x for x in df.CharacteristicName.unique() if x not in md.DataName.unique()]
    if len(mismatches) > 0:
        problems += len(mismatches)
        print(f'Dataframe characteristics not present in metadata:')
        print(mismatches)
    mismatches = [x for x in df.MonitoringLocationIdentifier.unique() if x not in md.SiteCodeWQX.unique()]
    if len(mismatches) > 0:
        problems += len(mismatches)
        print(f'Dataframe site IDs not present in metadata:')
        print(mismatches)
    mismatches = [x for x in md.SiteCodeWQX.unique() if x not in df.MonitoringLocationIdentifier.unique()]
    if len(mismatches) > 0:
        problems += len(mismatches)
        print(f'Metadata site IDs not present in dataframe:')
        print(mismatches)

    # spot-fixes
    md = _wqp_metadata_qc_spot_fixes(md)

    # mismatches between pairs of characteristics/units; do the char/unit pairs in metadata match the ones in wqp?
    problems+=_wqp_metadata_qc_check_sitechar_combinations(md,df)
    # problems+=_wqp_metadata_qc_check_charunit_combinations(md,df) # commented because units use different charsets, e.g., u versus `mu`

    md = md.sort_values(['SiteCode','CharacteristicName'])

    if problems == 0:
        print("Metadata file passed QC...")
    else:
        print("Metadata file failed QC!")

    return md

def _metadata_template(md:pd.DataFrame) -> pd.DataFrame:
    
    template = pd.DataFrame(columns=md.columns)
    template['Network'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['Network']
    template['DataName'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['DataName']
    template['Category'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['Category']
    template['CategoryDisplay'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['CategoryDisplay']
    template['Units'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['Units']
    template['CharacteristicName'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['CharacteristicName']
    template['DisplayName'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['DisplayName']
    template['Type'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['Type']
    template['DataName'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['DataName']
    template['DataType'] = md[md['SiteCodeWQX']==md['SiteCodeWQX'].unique()[0]]['DataType']

    return template

def _wqp_metadata_site_incongruency(df:pd.DataFrame, md:pd.DataFrame) -> pd.DataFrame:

    # Incongruencies can go in either of two directions: adds or deletes
    deletes = [x for x in md.SiteCodeWQX.unique() if x not in df.MonitoringLocationIdentifier.unique()] # anything present in md but absent from df should be deleted from md
    mask = (md['SiteCodeWQX'].isin(deletes)==False)
    md = md[mask].reset_index(drop=True)

    adds = [x for x in df.MonitoringLocationIdentifier.unique() if x not in md.SiteCodeWQX.unique()] # anything present in df but absent in md should be added to md
    
    # make a template: each site needs the exact columns and rows present in the template
    template = _metadata_template(md)

    lu = md[['ParkCode','ShortName','LongName']].drop_duplicates('ShortName')
    # for each site that needs to be added, fill-in the template and append to the metadata file
    for a in adds:
        mask = (df['MonitoringLocationIdentifier']==a)
        # base cases
        sitecode = None
        sitecodewqx = None
        sitename = None
        parkcode = None
        shortname = None
        longname = None
        lat = np.NaN
        lon = np.NaN

        # cols to fill into template
        # SiteCode, SiteCodeWQX, ParkCode
        sitecodes = df[mask].MonitoringLocationIdentifier.unique()
        if len(sitecodes)==1:
            sitecode = sitecodes[0]
            sitecodewqx = sitecodes[0]
            parkcode = sitecode.split('_')[1]
        # SiteName
        sitenames = df[mask].MonitoringLocationName.unique()
        if len(sitenames) == 1:
            sitename = sitenames[0]
        # ShortName and LongName
        if parkcode is not None and parkcode in lu.ParkCode.unique():
            shortname = lu[lu['ParkCode']==parkcode].ShortName.unique()[0]
            longname = lu[lu['ParkCode']==parkcode].LongName.unique()[0]
        # Lat
        lats = df[mask]['ActivityLocation/LatitudeMeasure'].unique()
        if len(lats) == 1:
            lat = lats[0]
        # Long
        lons = df[mask]['ActivityLocation/LongitudeMeasure'].unique()
        if len(lons) == 1:
            lon = lons[0]

        # QC; if the algo can't figure out the "right" answer, just have the user input the right answer
        if sitecode is None:
            print(f'WARNING: failed to parse {a}: {sitecodes=}')
            sitecode = input(f'Enter a sitecode for {a}. e.g., NCRN_XXXX_YYYY')
            sitecodewqx = sitecode
            parkcode = sitecode.split('_')[1]
        if sitename is None:
            print(f'WARNING: failed to parse {a}: {sitenames=}')
            sitename = input(f'Enter a sitename for {a}. e.g., Luzon Branch')
        if shortname is None:
            print(f'WARNING: failed to parse {a}: {shortname=}')
            shortname = input(f'Enter a Park shortname for {a}. e.g., Harpers Ferry')
        if longname is None:
            print(f'WARNING: failed to parse {a}: {longname=}')
            longname = input(f'Enter a Park shortname for {a}. e.g., Harpers Ferry National Historical Park')
        if np.isnan(lat):
            print(f'WARNING: failed to parse {a}: {lats=}')
            lat = input(f'Enter a site latitude (dec deg) for {a}. e.g., 38.557744')
        if np.isnan(lon):
            print(f'WARNING: failed to parse {a}: {lons=}')
            lon = input(f'Enter a site latitude (dec deg) for {a}. e.g., -77.792274')

        # assign
        template['SiteCode'] = sitecode
        template['SiteCodeWQX'] = sitecodewqx
        template['ParkCode'] = parkcode
        template['SiteName'] = sitename
        template['ShortName'] = shortname
        template['LongName'] = longname
        template['Lat'] = lat
        template['Long'] = lon

        md = pd.concat([md,template]).reset_index(drop=True).sort_values('DataName')

    return md

def water_sites(out_filename:str) -> pd.DataFrame:
    """Make a user-friendly dataframe of NCRN Water monitoring locations

    Included site attributes:
        PROTOCOL: str; the name of the NCRN monitoring protocol
        GROUPCODE: str; the four-letter code for the Park
        GROUPNAME: str; the Park formal name
        UNITCODE: str; the four-letter code for the Park or sub-park-unit, if relevant
        UNITNAME: str; the Park or sub-park-unit formal name
        IMLOCID: str; the location identifier in format "NCRN", "GROUPCODE", "site code"
        SITENAME: str; the site formal name
        LON_DECDEG: float; site longitude in decimal degrees
        LAT_DECDEG: float; site latitude in decimal degrees

    Args:
        out_filename (str): relative or absolute filepath to save the file

    Returns:
        pd.DataFrame: dataframe of NCRN Water monitoring locations

    Examples:
        import utils
        df = utils.water_sites('mydf.csv')
    """
    assert out_filename.endswith('.csv')

    df = pd.read_csv(r"data\ECO_MonitoringLocations_pt_0.csv")
    # TODO: replace this pd.read_csv() call with a water_backup._agol_hosted_feature() call so the sites stay in-sync with prod data
    mask = (df['PROTOCOL']=='Water') & (df['ISEXTANT']=='TRUE')
    df = df[mask][['PROTOCOL','GROUPCODE','GROUPNAME','UNITCODE','UNITNAME','IMLOCID','SITENAME','X','Y']].drop_duplicates('IMLOCID').sort_values('GROUPCODE')
    df.rename(columns={'X':'LON_DECDEG','Y':'LAT_DECDEG'}, inplace=True)
    df.to_csv(out_filename, index=False)

    return df  

def wqp_wqx(test_run:bool=False) -> pd.DataFrame:
    
    include_deletes:bool=False
    
    # Extract steps
    newest_data_folder:str = _find_newest_folder(assets.DM_WATER_BACKUP_FPATH) # find the newest timestamp folder
    df_dict:dict = _extract(newest_data_folder) # extract csvs to dataframes
    
    # Transform steps
    df:pd.DataFrame = tf._transform(df_dict=df_dict, include_deletes=include_deletes)
        # QC checks
    df = tf._quality_control(df)
    df = _wqp_qc(df=df)
    df = tf._assign_activity_id(df=df)
    df = tf._add_methodspeciationname(df=df)

    # import the example file
    example:pd.DataFrame = pd.read_csv(assets.EXAMPLE_WQX_WQP, nrows=1)
    if 'Unnamed: 0' in example.columns:
        del example['Unnamed: 0']

    # crosswalk columns
    wqp:pd.DataFrame = pd.DataFrame(columns=example.columns)

    xwalk = {
        'cols':{ # columns that have a 1:1 match between the NCRN dataframe and wqp; these determine nrow in the output wqp dataframe
            #  colname from wqp : colname from df
            'ActivityIdentifier':'activity_id'
            ,'ActivityMediaSubdivisionName':'activity_group_id'
            ,'ActivityStartDate':'activity_start_date'
            ,'ActivityStartTime/Time':'activity_start_time'
            ,'ActivityStartTime/TimeZoneCode':'timezone'
            ,'MonitoringLocationIdentifier':'location_id'
            ,'MonitoringLocationName':'ncrn_site_name'
            # ,'ActivityCommentText':'site_visit_notes'
            ,'ActivityLocation/LatitudeMeasure':'ncrn_latitude'
            ,'ActivityLocation/LongitudeMeasure':'ncrn_longitude'
            ,'ResultDetectionConditionText':'data_quality_flag'
            ,'CharacteristicName':'Characteristic_Name'
            ,'ResultMeasureValue':'Result_Text'
            ,'ResultMeasure/MeasureUnitCode':'Result_Unit'
            ,'ResultAnalyticalMethod/MethodIdentifier':'grouping_var'
            ,'LaboratoryName':'lab'
            ,'MethodSpeciationName':'MethodSpeciationName'
            ,'ResultSampleFractionText':'ResultSampleFractionText'
            ,'SampleCollectionEquipmentName':'instrument'
        }
        ,'constants':{ # columns that are constants and need to repeat nrow times in the output wqp dataframe
            # colname from wqp : value to assign to that column
            'OrganizationIdentifier':'NCRN'
            ,'OrganizationFormalName':'National Park Service Inventory and Monitoring Division'
            ,'ActivityCommentText':None # do not export comments
            # ,'ActivityMediaName':'Water'
            ,'ActivityEndDate':np.NaN
            ,'ActivityEndTime/Time':np.NaN
            ,'ActivityEndTime/TimeZoneCode':None
            ,'ActivityRelativeDepthName':None
            ,'ActivityDepthHeightMeasure/MeasureValue':np.NaN
            ,'ActivityDepthHeightMeasure/MeasureUnitCode':None
            ,'ActivityDepthAltitudeReferencePointText':None
            ,'ActivityTopDepthHeightMeasure/MeasureValue':np.NaN
            ,'ActivityTopDepthHeightMeasure/MeasureUnitCode':None
            ,'ActivityTopDepthHeightMeasure/MeasureUnitCode':None
            ,'ActivityBottomDepthHeightMeasure/MeasureValue':np.NaN
            ,'ActivityBottomDepthHeightMeasure/MeasureUnitCode':None
            ,'ProjectIdentifier':'USNPS NCRN Perennial stream water monitoring'
            ,'ProjectName':'USNPS NCRN Perennial stream water monitoring'
            ,'ActivityConductingOrganizationText':'USNPS National Capital Region Inventory and Monitoring'
            ,'SampleAquifer':None
            ,'HydrologicCondition':None
            ,'HydrologicEvent':None
            ,'SampleCollectionMethod/MethodIdentifier':None
            ,'SampleCollectionMethod/MethodIdentifierContext':None
            ,'SampleCollectionMethod/MethodName':np.NaN
            ,'SampleCollectionMethod/MethodDescriptionText':np.NaN
            # ,'SampleCollectionEquipmentName':None
            # ,'MethodSpeciationName':None
            # ,'ResultSampleFractionText':None
            ,'MeasureQualifierCode':None
            # ,'ResultCommentText':'Final'
            ,'StatisticalBaseCode':None
            # ,'ResultValueTypeName':'Actual'
            ,'ResultWeightBasisText':None
            ,'ResultTemperatureBasisText':None
            ,'ResultParticleSizeBasisText':None
            ,'DataQuality/PrecisionValue':np.NaN
            ,'DataQuality/BiasValue':np.NaN
            ,'DataQuality/ConfidenceIntervalValue':np.NaN
            ,'DataQuality/UpperConfidenceLimitValue':np.NaN
            ,'DataQuality/LowerConfidenceLimitValue':np.NaN
            ,'ResultCommentText':None
            ,'ResultStatusIdentifier':'Final'
            ,'USGSPCode':None
            ,'ResultDepthHeightMeasure/MeasureValue':np.NaN
            ,'ResultDepthHeightMeasure/MeasureUnitCode':None
            ,'ResultDepthAltitudeReferencePointText':None
            ,'SubjectTaxonomicName':None # is NA for water but is not NA for BSS
            ,'SampleTissueAnatomyName':None
            ,'BinaryObjectFileName':None
            ,'BinaryObjectFileTypeCode':None
            ,'ResultFileUrl':None # TODO: update to data package?
            ,'ResultAnalyticalMethod/MethodIdentifierContext':None
            ,'ResultAnalyticalMethod/MethodName':None
            ,'ResultAnalyticalMethod/MethodUrl':None
            ,'ResultAnalyticalMethod/MethodDescriptionText':None
            ,'AnalysisStartDate':np.NaN
            ,'ResultLaboratoryCommentText':None
            ,'ResultDetectionQuantitationLimitUrl':None
            ,'DetectionQuantitationLimitTypeName':None
            ,'DetectionQuantitationLimitMeasure/MeasureValue':np.NaN # TODO: update with real values
            ,'DetectionQuantitationLimitMeasure/MeasureUnitCode':None
            ,'LabSamplePreparationUrl':None
            ,'LastUpdated':dt.datetime.now()
            ,'ProviderName':'National Park Service Inventory and Monitoring Division'
            ,'ResultTimeBasisText':None
        }
        ,'calculated':{ # columns that need to be re-calculated each time the dataset is produced
            'ResultIdentifier':'wqp["ResultIdentifier"]=wqp.index'
            ,'ActivityMediaName' : "wqp['ActivityMediaName'] = np.where(wqp['CharacteristicName'].isin(['air_temperature', 'barometric_pressure', 'weather_condition']), 'Air', 'Water')"
            ,'ActivityTypeCode' : "wqp['ActivityTypeCode'] = np.where(wqp['ResultAnalyticalMethod/MethodIdentifier']=='NCRN_WQ_WCHEM', 'Sample-Routine', 'Field Msr/Obs')"
            ,'ResultValueTypeName': "wqp['ResultValueTypeName'] = np.where(wqp['SampleCollectionEquipmentName'] == 'calculated_result','Calculated','Actual')"
            # ,'ActivityIdentifier':'wqp["ActivityIdentifier"]=wqp["ActivityMediaSubdivisionName"]+"|"+wqp["ResultAnalyticalMethod/MethodIdentifier"]'
        }
    }
    assert len(xwalk['cols']) + len(xwalk['constants']) + len(xwalk['calculated']) == len(example.columns) # sanity check that the `xwalk`` is complete

    # assign based on xwalk
    for k in xwalk.keys():
        if k == 'cols':
            for x,y in xwalk[k].items():
                try:
                    wqp[x] = df[y]
                except:
                    print(f'failed: {x=} and {y=}')
        elif k == 'constants':
            for x,y in xwalk[k].items():
                try:
                    wqp[x] = y
                except:
                    print(f'failed: {x=} and {y=}')
        elif k == 'calculated':
            for x, y in xwalk[k].items():
                try:
                    exec(y)
                except:
                    print(f"WARNING! Calculated column `xwalk['{k}']['{x}']`, code line `{y}` failed.")
    
    wqp = _recode_wqp_chars(wqp=wqp)

    # TODO: write wqp csv if test_run == False
    if test_run == False:
        fname = f'{newest_data_folder}\\wqp.csv'
        wqp.to_csv(fname, index=False)
        print(f'\nWrote wqp to: {fname}\n')
        mdfname = f'{newest_data_folder}\\wqp_ncrnwater_metadata.csv'
        md = wqp_metadata(df=fname, write=mdfname)

    # TODO: make metadata and write metadata to the same file as wqp csv
    # md = wqp_metadata()

    return wqp

def _recode_wqp_chars(wqp:pd.DataFrame) -> pd.DataFrame:
    """Recode the characteristic names from NCRN charnames to WQP charnames

    Hard-coded values from: https://www.epa.gov/waterdata/water-quality-data-upload-wqx#wqxoverview:~:text=Quick%20WQX%20Web%20User%20guide

    Args:
        wqp (pd.DataFrame): A dataframe of preprocessed NCRN water records in WQP columns

    Returns:
        pd.DataFrame: The input dataframe with characteristic names recoded from NCRN aliases to WQP `CharacteristicName` values
    """
    # make a lookup table so that 
    lu = pd.DataFrame({'ncrn':wqp.CharacteristicName.unique()})
    lu['wqp'] = None

    # hard code the values into the lookup table
    # https://www.epa.gov/waterdata/water-quality-data-upload-wqx#wqxoverview:~:text=Quick%20WQX%20Web%20User%20guide
    lu['wqp'] = np.where(lu['ncrn']=='air_temperature','Temperature, air',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='discharge','Base flow discharge',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='mean_crossection_depth','Cross-Section Depth',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='water_temperature','Temperature, water',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='ph','pH',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='specific_conductance','Specific conductance',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='do_concentration','Dissolved oxygen (DO)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='do_saturation','Dissolved oxygen saturation',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='anc','Acid Neutralizing Capacity (ANC)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='stream_physical_appearance','Water appearance (text)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='tp','Total Phosphorus, mixed forms',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='ammonia','Ammonia',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='nitrate','Nitrate',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='wetted_width','Wetted Width',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='mean_velocity','Velocity - stream',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='barometric_pressure','Barometric pressure',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='conductivity','Conductivity',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='salinity','Salinity',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='tds','Total dissolved solids',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='tn','Total Nitrogen, mixed forms',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='orthophosphate','Orthophosphate',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='tdn','Total Dissolved Nitrogen, mixed forms',lu['wqp']) # there is not a 1:1 equivalent for this in wqp
    lu['wqp'] = np.where(lu['ncrn']=='tdp','Total Dissolved Phosphorus, mixed forms',lu['wqp']) # there is not a 1:1 equivalent for this in wqp
    lu['wqp'] = np.where(lu['ncrn']=='weather_condition','Weather condition (WMO code 4501) (choice list)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='rain_last_24','RBP2, Weather Condition, Heavy Rain in Last 7 Days, Y/N (choice list)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='algae_cover_percent','Substrate algae, % (choice list)',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='algae_description','Substrate algae color',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='flow_status','Stream flow (choice list)',lu['wqp'])
    # lu['wqp'] = np.where(lu['ncrn']=='chlorine','Chlorine',lu['wqp'])
    lu['wqp'] = np.where(lu['ncrn']=='turbidity','Turbidity',lu['wqp'])

    # sanity check that the lu is complete
    missing = lu[lu['wqp'].isna()]
    assert len(missing) == 0, print(f'FAIL! {len(missing)} values are present in the dataset but missing from the lookup. See `_recode_wqp_chars()`\n\n{missing.ncrn.unique()}')

    # sanity check that every value in the lu is unique
    dupes = lu.drop_duplicates('ncrn')
    assert len(dupes) == len(lu), print(f'FAIL! there are duplicates in the lookup. See `_recode_wqp_chars()')
    dupes = lu.drop_duplicates('wqp')
    assert len(dupes) == len(lu), print(f'FAIL! there are duplicates in the lookup. See `_recode_wqp_chars()')

    for x in lu.ncrn.values:
        mask = (wqp['CharacteristicName']==x)
        replacement = lu[lu['ncrn']==x].wqp.unique()[0]
        wqp['CharacteristicName'] = np.where(mask, replacement, wqp['CharacteristicName'])

    return wqp

def _wqp_qc(df:pd.DataFrame) -> pd.DataFrame:
    """Enforce the quality control rules that are relevant for only the WQP version of the data

    Args:
        df (pd.DataFrame): dataframe in the output format from tf._transform()

    Returns:
        pd.DataFrame: dataframe in WQP format
    """

    # exclude unverified and review-in-progress records
    df = df[df['review_status']=='verified']
    df.reset_index(inplace=True, drop=True)

    # exclude characteristics by name
    excludes = [
        'left_bank_riparian_width'
        ,'right_bank_riparian_width'
        ,'duplicate_y_n'
        ,'nutrient_bottle_size'
        ,'anc_bottle_size'
        ,'ysi_increment_distance'
        ,'entry_other_stream_phy_appear'
        ,'discharge_instrument'
        ,'ysi_probe'
        ,'tape_offset'
        ,'rain_last_24'
        ,'chlorine'
    ]
    mask = (df['Characteristic_Name'].isin(excludes)==False)
    df = df[mask]

    # re-code lab names per time period
    # activity_start_date < 2016-10-01 ~'CUE'
    cue_end = dt.date(2016,10,1)
    mask_cue = (pd.to_datetime(df['activity_start_date']).dt.date < cue_end) & (df['grouping_var']=='NCRN_WQ_WCHEM')
    # activity_start_date >= 2016-10-01 & activity_start_date <= '2020-01-01 ~ 'CBL'
    cbl_start = cue_end
    cbl_end = dt.date(2020,1,1)
    mask_cbl = (pd.to_datetime(df['activity_start_date']).dt.date >= cbl_start) & (pd.to_datetime(df['activity_start_date']).dt.date <= cbl_end) & (df['grouping_var']=='NCRN_WQ_WCHEM')
    # activity_start_date > 2020-01-01 ~ 'AL'
    al_start = cbl_end
    mask_al = (pd.to_datetime(df['activity_start_date']).dt.date > al_start) & (df['grouping_var']=='NCRN_WQ_WCHEM')

    df['lab'] = np.where(mask_cue, 'CUE', df['lab'])
    df['lab'] = np.where(mask_cbl, 'CBL', df['lab'])
    df['lab'] = np.where(mask_al, 'AL', df['lab'])

    # remove CUE phosphorus results
    phosphorus_chars = ['orthophosphate', 'tp', 'tdp', 'ammonia']
    mask = (df['lab']=='CUE') & (df['Characteristic_Name'].isin(phosphorus_chars))
    df = df[~mask]

    # remove CUE nitrogen results
    nitrogen_chars = ['nitrate', 'tn', 'tdn']
    mask = (df['lab']=='CUE') & (df['Characteristic_Name'].isin(nitrogen_chars))
    df = df[~mask]

    # case-statements to fix flagging ambiguity
    
    # 'equipment_malfunction'
    # When the result is flagged as 'equipment_malfunction', the result should be updated to NA.
    mask = (df['data_quality_flag']=='equipment_malfunction') & (df['Result_Text'].isna()==False)
    # mask = (df['ResultDetectionConditionText']=='equipment_malfunction') & (df['ResultMeasureValue'].isna()==False)
    df['Result_Text'] = np.where(mask, None, df['Result_Text'])

    # present_not_on_datasheet
    # Calculated results. 2007-12-18 is the earliest site visit with the "new" style of pdf.
    # For >=2007-12-18, calculated results do not need to be flagged. update the flag to NA
    # For <2007-12-18, calculated results need to be flagged
    cutoff_date = dt.date(2007,12,18)
    mask = (pd.to_datetime(df['activity_start_date']).dt.date >= cutoff_date) & (df['data_quality_flag']=='present_not_on_datasheet') & (df['instrument']=='calculated_result')
    # mask = (pd.to_datetime(df['ActivityStartDate']).dt.date >= cutoff_date) & (df['ResultDetectionConditionText']=='present_not_on_datasheet') & (df['SampleCollectionEquipmentName']=='calculated_result')
    df['data_quality_flag'] = np.where(mask, None, df['data_quality_flag'])
    # QA results
    # When these results are part of the 'QA' increment for the YSI, we will remove the flag, export the result, and indicate 'QA' in the appropriate WQP column
    mask = (df['ysi_increment_notes'].str.contains('QA')) & (df['grouping_var']=='NCRN_WQ_WQUALITY') & (df['data_quality_flag']=='present_not_on_datasheet')
    # mask = (df['ResultDetectionConditionText']=='QA; repeated sample at same location')
    df['data_quality_flag'] = np.where(mask, 'QA; repeated sample at same location', df['data_quality_flag'])

    df.reset_index(inplace=True, drop=True)

    return df

def _extract(newest_data_folder:str) -> dict:
    """Call-stacking function for extract steps

    Args:
        data_folder (str): relative or absolute filepath to a folder containing timestamped folders containing the .zip csv-collection downloaded from AGOL.

    Returns:
        dict: A dictionary of NCRN water monitoring data in relational form; one table in the .zip form `data_folder` becomes one key-value pair in the dictionary.
    """

    # look at the contents of that newest folder and find a .zip file with 'csv' in the filename
    targets = os.listdir(newest_data_folder)
    targets = [x for x in targets if x.endswith('.zip') and 'csv' in x]
    assert len(targets) > 0, print(f'Returned zero csv collections in {newest_data_folder=}')
    target = os.path.join(newest_data_folder, max(targets))

    # unzip the files
    newdir = target.rsplit('.zip',1)[0].rsplit('\\',1)[-1]
    shutil.unpack_archive(target, os.path.join(newest_data_folder, newdir))

    # extract each table
    extracted_dir = os.path.join(newest_data_folder, newdir)
    tbls = os.listdir(extracted_dir)
    tbls_fnames = [x for x in tbls if '_0' in x or 'ysi' in x or 'grabsample' in x]
    for i in range(len(tbls_fnames)):
        if '_0' in tbls_fnames[i]:
            tbls_fnames[i] = 'tbl_main'
        else:
            tbls_fnames[i] = tbls_fnames[i].rsplit('_',1)[0]
    tbls_fpaths = [os.path.join(extracted_dir, x) for x in tbls if '_0' in x or 'ysi' in x or 'grabsample' in x]
    assert len(tbls_fnames) == len(tbls_fpaths)
    df_dict:dict = {tbls_fnames[i]: {'fpath':tbls_fpaths[i], 'df':pd.read_csv(tbls_fpaths[i])} for i in range(len(tbls_fnames))}

    return df_dict

def _find_newest_folder(data_folder:str) -> str:

    # find the newest folder in a given folder
    # use the filenames to find the newest timestamp
    dirs = [x for x in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder,x))]
    newest_data_folder = os.path.join(data_folder, max(dirs))
    assert os.path.isdir(newest_data_folder), print(f'data folder {newest_data_folder=} does not exist')

    return newest_data_folder

