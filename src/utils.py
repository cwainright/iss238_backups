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
    utils.backup_water(verbose=True, test_run=False) # query prod
    utils.dashboard_etl(test_run=False, include_deletes=False, verbose=True) # query prod

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

def backup_water(dest_dir:str=assets.DM_WATER_BACKUP_FPATH, verbose:bool=False, test_run:bool=False) -> None:
    """Generic to make backups of NCRN water source files

    Args:
        dest_dir (str, optional): Absolute or relative filepath to receive the backup files. Defaults to assets.DM_WATER_BACKUP_FPATH.
        verbose (bool, optional): True turns on interactive messaging. Defaults to False.
        test_run (bool, optional): True points to development source files, False points to production. Defaults to False.

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
    if test_run==True:
        dirs=assets.SURVEY_DEV_DIRS
    else:
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
        src = assets.WATER_DEV_ITEM_ID
    else:
        src = assets.WATER_AGOL_ITEM_ID
    # newpath:str = _make_new_backup_dir(dest_dir=dest_dir, verbose=verbose, dir_ext=dir_ext)
    wtb._agol_hosted_feature(newpath=newpath, in_fc=src, verbose=verbose, dir_ext=dir_ext)
    
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

def dashboard_etl(test_run:bool=False, include_deletes:bool=False, verbose:bool=False) -> pd.DataFrame:
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
    
    # QC checks
    if verbose == True:
        tf._quality_control(df)

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

def wqp_wqx(test_run:bool=False, include_deletes:bool=False, verbose:bool=False) -> pd.DataFrame:
    # Extract steps
    newest_data_folder:str = _find_newest_folder(assets.DM_WATER_BACKUP_FPATH) # find the newest timestamp folder
    df_dict:dict = _extract(newest_data_folder) # extract csvs to dataframes
    
    # Transform steps
    df:pd.DataFrame = tf._transform(df_dict=df_dict, include_deletes=include_deletes)

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
    ]
    mask = (df['Characteristic_Name'].isin(excludes)==False)
    df = df[mask]
    df.reset_index(inplace=True, drop=True)

    # import the example file
    example:pd.DataFrame = pd.read_csv(assets.EXAMPLE_WQX_WQP)
    if 'Unnamed: 0' in example.columns:
        del example['Unnamed: 0']

    # alias characteristics from NCRN characteristic names to wqp names
    # example[example['ResultAnalyticalMethod/MethodIdentifier']=='NCRN_WQ_LAB'].CharacteristicName.unique()

    # pivot a column to a characteristic
    pivots = [
        'sampleability'
        ,'visit_type'
    ]
    for c in pivots:
        ncol = len(df.columns)
        nrow = len(df)

        tmp = df.copy()
        tmp = tmp.drop_duplicates('activity_group_id')
        tmp['Characteristic_Name'] = c
        tmp['str_result'] = df[c]
        tmp['high']=np.NaN
        tmp['low']=np.NaN
        tmp['result_warning']=None
        tmp['data_type'] = 'string'
        tmp['Result_Unit'] = None
        tmp['num_result'] = np.NaN
        df = pd.concat([df,tmp])
        df.reset_index(inplace=True, drop=True)

        assert len(df.columns) == ncol # sanity check
        assert len(df) > nrow # sanity check

    # pivot a characteristic to a column
    pivots = [
        'lab'
    ]

    for c in pivots:
        ncol = len(df.columns)
        nrow = len(df)
        
        tmp = df.copy()
        mask = (df['Characteristic_Name']==c)
        df = df[~mask].copy()
        tmp = tmp[mask].copy()
        mask = (tmp['str_result'].isna()==False)
        tmp = tmp[mask].copy()
        tmp = tmp.drop_duplicates('activity_group_id')
        tmp = tmp[['activity_group_id','str_result']]
        tmp.rename(columns={'str_result':'lab'}, inplace=True)

        df = pd.merge(left=df, right=tmp, how='left', on='activity_group_id')
        df.reset_index(drop=True, inplace=True)

    assert len(df.columns) == ncol+len(pivots) # sanity check
    assert len(df) < nrow # sanity check

    # make a lookup
    lookup = {}
    allchars = []
    for x in example['ResultAnalyticalMethod/MethodIdentifier'].unique():
        if str(x).startswith('NCRN_WQ'):
            lookup[x]={}
            mask = (example['ResultAnalyticalMethod/MethodIdentifier']==x)
            chars = example[mask].CharacteristicName.unique()
            for c in chars:
                lookup[x][c] = None

    lookup['NCRN_WQ_LAB']['Acid Neutralizing Capacity (ANC)'] = 'anc'
    lookup['NCRN_WQ_LAB']['Total Nitrogen, mixed forms']='tn'
    lookup['NCRN_WQ_LAB']['Nitrogen, ammonia (NH3) as NH3']='ammonia'
    lookup['NCRN_WQ_LAB']['Total Phosphorus, mixed forms']='tp'
    lookup['NCRN_WQ_LAB']['Phosphorus, orthophosphate as PO4']='orthophosphate'
    lookup['NCRN_WQ_LAB']['Chlorine']='chlorine'
    lookup['NCRN_WQ_LAB']['Nitrogen, Nitrate']='nitrate'
    lookup['NCRN_WQ_LAB']['Nitrogen, dissolved']='tdn'
    lookup['NCRN_WQ_LAB']['Phosphorus, dissolved']='tdp'
    lookup['NCRN_WQ_LAB']['Turbidity']='turbidity'
    lookup['NCRN_WQ_KES']['Temperature, air']='air_temperature'
    lookup['NCRN_WQ_HABINV']['Algae, substrate rock/bank cover (choice list)']='algae_cover_percent'
    lookup['NCRN_WQ_HABINV']['Algae description']='algae_description'
    lookup['NCRN_WQ_HABINV']['Stream Physical Appearance (choice list)']='stream_physical_appearance'
    lookup['NCRN_WQ_HABINV']['RBP Channel Flow Status (choice list)']='flow_status'
    lookup['NCRN_WQ_HABINV']['sampleability']='sampleability'
    lookup['NCRN_WQ_HABINV']['visit type']='visit_type'
    lookup['NCRN_WQ_HABINV']['rain in last 24 hours']='rain_last_24'
    lookup['NCRN_WQ_HABINV']['photos taken at site']='photos_y_n'
    lookup['NCRN_WQ_HABINV']['Weather Condition (WMO Code 4501) (choice list)']='weather_condition'
    lookup['NCRN_WQ_HABINV']['RBP2, Low G, Riparian Vegetative Zone Width, Left Bank (choice list)']=None
    lookup['NCRN_WQ_HABINV']['RBP2, Low G, Riparian Vegetative Zone Width, Right Bank (choice list)']=None
    lookup['NCRN_WQ_HABINV']['RBP2, Watershed, Predominant Surrounding Landuse (choice list)']=None
    lookup['NCRN_WQ_HABINV']['RBP2, Riparian Vegetation, Dominant Species Present (choice list)']=None
    lookup['NCRN_WQ_HABINV']['RBP Channelized Y/N (choice list)']=None
    lookup['NCRN_WQ_HABINV']['Bank erosion stability (choice list)']=None
    lookup['NCRN_WQ_FLTK']['Wetted Width']='wetted_width'
    lookup['NCRN_WQ_FLTK']['Base flow discharge']='discharge'
    lookup['NCRN_WQ_FLTK']['Cross-Section Depth']='mean_crossection_depth'
    lookup['NCRN_WQ_FLTK']['RBP Stream Velocity']='mean_velocity'
    lookup['NCRN_WQ_YSI']['Description of sequential increment from right bank']='ysi_increment'
    lookup['NCRN_WQ_YSI']['Solids, Dissolved (TDS)']='tds'
    lookup['NCRN_WQ_YSI']['Dissolved oxygen (DO)']='do_concentration'
    lookup['NCRN_WQ_YSI']['Salinity']='salinity'
    lookup['NCRN_WQ_YSI']['Conductivity']='conductivity'
    lookup['NCRN_WQ_YSI']['Temperature, water']='water_temperature'
    lookup['NCRN_WQ_YSI']['Specific conductance']='specific_conductance'
    lookup['NCRN_WQ_YSI']['pH']='ph'
    lookup['NCRN_WQ_YSI']['Dissolved oxygen saturation']='do_saturation'
    lookup['NCRN_WQ_YSI']['Barometric pressure']='barometric_pressure'

    # clean the lookup table
    tmp = {}
    for k in lookup.keys():
        tmp[k] = {}
        for x,y in lookup[k].items():
            if y is not None:
                tmp[k][x]=y
    lookup = tmp.copy()
    for k in lookup.keys():
        for x,y in lookup[k].items():
            allchars.append(y)
    assert len([x for x in df.Characteristic_Name.unique() if x not in allchars]) == 0 # sanity check # check that the lookup is complete
    
    # pivot the lookup table
    lu = pd.DataFrame(lookup).reset_index()
    lu.rename(columns={'index':'CharacteristicName'},inplace=True)
    myids = ['CharacteristicName']
    lu = lu.melt(
        id_vars=myids
        ,value_vars=[x for x in lu.columns if x not in myids]
        ,var_name='method_identifier'
        ,value_name='Characteristic_Name'
        )
    lu = lu[lu['Characteristic_Name'].isna()==False]
    lu.reset_index(drop=True, inplace=True)
    
    # assign example['ResultAnalyticalMethod/MethodIdentifier'].unique() based on the lookup table
    nrow=len(df)
    ncol=len(df.columns)
    df = pd.merge(left=df, right=lu, how='left', on='Characteristic_Name')
    assert len(df) == nrow # sanity check
    assert len(df.columns) == ncol+2 # sanity check

    # crosswalk columns
    wqp:pd.DataFrame = pd.DataFrame(columns=example.columns)

    xwalk = {
        'cols':{ # columns that have a 1:1 match between the NCRN dataframe and wqp; these determine nrow in the output wqp dataframe
            #  colname from wqp : colname from df
            'ActivityIdentifier':'activity_group_id'
            ,'ActivityMediaSubdivisionName':'activity_group_id'
            ,'ActivityStartDate':'activity_start_date'
            ,'ActivityStartTime/Time':'activity_start_time'
            ,'ActivityStartTime/TimeZoneCode':'timezone'
            ,'MonitoringLocationIdentifier':'location_id'
            ,'MonitoringLocationName':'ncrn_site_name'
            ,'ActivityCommentText':'site_visit_notes'
            ,'ActivityLocation/LatitudeMeasure':'ncrn_latitude'
            ,'ActivityLocation/LongitudeMeasure':'ncrn_longitude'
            ,'ResultDetectionConditionText':'data_quality_flag'
            ,'CharacteristicName':'CharacteristicName'
            ,'ResultMeasureValue':'str_result'
            ,'ResultMeasure/MeasureUnitCode':'Result_Unit'
            ,'ResultAnalyticalMethod/MethodIdentifier':'method_identifier'
            ,'LaboratoryName':'lab'
        }
        ,'constants':{ # columns that are constants and need to repeat nrow times in the output wqp dataframe
            # colname from wqp : value to assign to that column
            'OrganizationIdentifier':'NCRN'
            ,'OrganizationFormalName':'National Park Service Inventory and Monitoring Division'
            ,'ActivityTypeCode':'Field Msr/Obs'
            ,'ActivityMediaName':'Water'
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
            ,'SampleCollectionEquipmentName':None
            ,'MethodSpeciationName':None
            ,'ResultSampleFractionText':None
            ,'MeasureQualifierCode':None
            ,'ResultStatusIdentifier':'Final'
            ,'StatisticalBaseCode':None
            ,'ResultValueTypeName':'Actual'
            ,'ResultWeightBasisText':None
            ,'ResultTemperatureBasisText':None
            ,'ResultParticleSizeBasisText':None
            ,'DataQuality/PrecisionValue':np.NaN
            ,'DataQuality/BiasValue':np.NaN
            ,'DataQuality/ConfidenceIntervalValue':np.NaN
            ,'DataQuality/UpperConfidenceLimitValue':np.NaN
            ,'DataQuality/LowerConfidenceLimitValue':np.NaN
            ,'ResultCommentText':None
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
        }
    }
    assert len(xwalk['cols']) + len(xwalk['constants']) + len(xwalk['calculated']) == len(example.columns) # sanity check that the `xwalk`` is complete
    # presentcols = []
    # for k in xwalk.keys():
    #     for x,y in xwalk[k].items():
    #         presentcols.append(x)
    # [x for x in example.columns if x not in presentcols]
    
    # assign based on xwalk
    for k in xwalk.keys():
        if k == 'cols':
            for x,y in xwalk[k].items():
                wqp[x] = df[y]
        elif k == 'constants':
            for x,y in xwalk[k].items():
                wqp[x] = y
        elif k == 'calculated':
            for x, y in xwalk[k].items():
                try:
                    exec(y)
                except:
                    print(f"WARNING! Calculated column `xwalk['{k}']['{x}']`, code line `{y}` failed.")


    # quality-control
    # does wqp have the same number of rows as df?
    # does wqp have the same columns as example? (ncol and colnames)
    # do we have nulls in non-nullable fields?
    # are our data quality flags uniform?

    # write to csv if test_run == False

    return wqp


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

