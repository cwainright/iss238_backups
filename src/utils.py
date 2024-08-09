import shutil
import os
import src.assets as assets
import src.water_backup as wtb
import src.transform as tf
import datetime as dt
import pandas as pd

"""
Demonstrates how to upload large file
# https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/files/upload_large.py


import os

from office365.sharepoint.client_context import ClientContext
from tests import test_team_site_url, test_user_credentials


def print_upload_progress(offset):
    # type: (int) -> None
    file_size = os.path.getsize(local_path)
    print(
        "Uploaded '{0}' bytes from '{1}'...[{2}%]".format(
            offset, file_size, round(offset / file_size * 100, 2)
        )
    )


ctx = ClientContext(test_team_site_url).with_credentials(test_user_credentials)

target_url = "Shared Documents/archive"
target_folder = ctx.web.get_folder_by_server_relative_url(target_url)
size_chunk = 1000000
local_path = "../../../tests/data/big_buck_bunny.mp4"
with open(local_path, "rb") as f:
    uploaded_file = target_folder.files.create_upload_session(
        f, size_chunk, print_upload_progress, "big_buck_bunny_v2.mp4"
    ).execute_query()

print("File {0} has been uploaded successfully".format(uploaded_file.serverRelativeUrl))
"""


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

        # example 1, copy AGOL assets to local folder called `output/` with interactive feedback `verbose` on.
        utils.backup_water(dest_dir='output', verbose=True)

        # example 2, copy AGOL assets to authoritative backup location with interactive feedback `verbose` off.
        utils.backup_water()

    """
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
    # _update_authoritative_dataset()
    # _update_dashboard_dataset()

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
    # check that the source directory and file exist
    assert os.path.exists(src_dir)==True, print(f'You provided {src_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert os.path.exists(dest_dir)==True, print(f'You provided {dest_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert len(filetypes)>0, print(f'You provided {filetypes=}, which is an empty list. Provide one or more file extensions to copy from `src_dir` to `dest_dir`.')

    # extend `dest_dir` with a timestamp and check that that directory does not exist
    dir_ext:str = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
    newpath:str = _make_new_backup_dir(dest_dir=dest_dir, verbose=verbose, dir_ext=dir_ext)
    _backup_make_file_copies(dir_ext=dir_ext, newpath=newpath, src_dir=src_dir, filetypes=filetypes, verbose=verbose)

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

def _update_authoritative_dataset():
    # take the existing WQX dataset, replace records in WQX with their verified dataset from agol
    # df_agol = _agol_to_wqx()
    # df_wqx = pd.read_csv(some source file)
    # _replace_wqx_with_agol() # 
    return None


def etl(data_folder:str, load:bool=False, feature_url:str='', include_deletes:bool=False) -> pd.DataFrame:
    """Extract-transform-load pipeline for ncrn discrete water data

    Args:
        data_folder (str): absolute or relative filepath to a folder containing the csv outputs from feature `ncrn_discrete_water_reviewer_20240112`
        load (bool, optional): Load the transformed data to a target feature service. Defaults to False.
        feature_url (str, optional): The target feature service to which the transformed data should be loaded. Defaults to ''.
        include_deletes (bool, optional): a flag to include the soft-deleted records. True includes soft-deleted records. False filters-out soft-deleted records.

    Returns:
        pd.DataFrame: flattened dataframe of ncrn water results, flattened and melted to long format for formatting and

    Examples:
        import src.utils as utils
        fpath = r'data\data_export_20240628'
        mydf = bu.etl(data_folder=fpath)

        bu.etl(data_folder=fpath, include_deletes=True)
    """
    if load == True:
        assert feature_url != '', print(f"You provided {feature_url}. If you want to load your transformed dataframe to a feature service, provide its url.")

    # Extract steps
    df_dict:dict = _extract(data_folder)
    
    # Transform steps
    df:pd.DataFrame = tf._transform(df_dict=df_dict, include_deletes=include_deletes)
    
    # Load steps
    # TODO: add load steps
    # if load == True:
    #     ld._load(df=df, feature_url=feature_url)
    #     print(f"Loaded feature to `{feature_url}`")

    # QC checks
    _quality_control(df)

    return df

def wqp_wqx(data_folder:str, include_deletes:bool=False) -> pd.DataFrame:
    # TODO: crosswalk the etl output into wqxwqp-output format
    df = pd.DataFrame()

    return df

def _quality_control(df:pd.DataFrame) -> pd.DataFrame:
    """Enforce business logic to quality-control the output of the pipeline"""

    nullables = [# columns that are nullable for all rows
        'paper_url1' # should be present for most records before 2018, but field will be blank until user reviews the record (reviewing the record triggers the logic that populates the field from the lookup table)
        ,'paper_url2' # will be NA for nearly all rows
        ,'analytical_method_id'
        ,'method_detection_limit' # 
        ,'review_notes'
    ]
    non_nullables = [x for x in df.columns if x not in nullables]
    for c in non_nullables:
        if df[c].isna().all():
            print("")
            print(f'WARNING (a): non-nullable field `{c}` is null in all rows')
            print("")


    # business rule-checking logic
    # if `review_status` IN ['verified', 'in_review'], the following fields are non-nullable

    statuses = ['verified', 'in_review']
    non_nullables = [
            'record_reviewers'
            ,'review_date'
            ,'review_time'
            ,'entry_review_date'
            ,'entry_review_time'
            ,'field_crew'
            ,'sampleability'
            ,'delete_record'
            ,'survey_complete'
            ,'form_version'
            ,'project_id'
            ,'skip_req_observations'
            ,'skip_req_ysi'
            ,'skip_req_flowtracker'
            ,'skip_req_grabsample'
            ,'skip_req_photo'
            ]

    for c in non_nullables:
        mask = (df['review_status'].isin(statuses)) & (df[c].isna()) & (df['delete_record'].isna()==False) & (df['delete_record']!='yes')
        if len(df[mask]) >0:
                print("")
                print(f'WARNING (b): non-nullable field `{c}` is null in {round(((len(df[mask]))/len(df)*100),2)}% of rows')
                print("")


    # # if `data_type` == 'float', the following fields are non-nullable
    #     [
    #         'Result_Unit'
    #     ]
    non_nullables = ['Result_Unit']
    for c in non_nullables:
        mask = (df['data_type']=='float') & (df[c].isna())
        if len(df[mask]) > 0:
                print("")
                print(f'WARNING (c): non-nullable field `{c}` is null in {round(((len(df[mask]))/len(df)*100),2)}% of rows')
                print("")

    # check for duplicate `activity_group_id`s for each `SiteVisitParentGlobalID`
    # TODO

    return df

def _extract(data_folder:str) -> pd.DataFrame:
    """Call-stacking function for extract steps

    Args:
        data_folder (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    # extract each table
    df_dict:dict = {}
    for tbl in assets.TBLS:
        tbl_filepath = f"{data_folder}\{tbl}.csv"
        df = pd.read_csv(tbl_filepath)
        df_dict[tbl] = df

    return df_dict