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

def dashboard_etl(dest_dir:str='', test_run:bool=False, load:bool=False, include_deletes:bool=False) -> pd.DataFrame:
    """Extract-transform-load pipeline that transforms relational NCRN discrete water data into one flat csv and optionally overwrites the feature service underlying the dashboard with the csv

    This pipeline finds the newest version of the data in a folder, extracts the newest data, transforms the data into the format for the backend of the QC dashboard.
    If `load` == True, the pipeline will overwrite the data in the feature service.
    If `dest_dir` != '', the pipeline will save the output into the folder specified.
    If `test_run` == True, the pipeline extracts data from a development asset; otherwise, the pipeline extracts data from the production asset.
    `include_deletes` applies cascade-deletes based on user-side fields (e.g., tbl_main.delete_record or tbl_ysi.delete_increment)

    Args:
        dest_dir (str, optional): relative or absolute filepath to a folder where you want to save the output file. Defaults to ''. If blank, will not write.
        test_run (bool, optional): True points `etl()` at development assets. False points `etl()` at production assets. Defaults to False.
        load (bool, optional): Load the transformed data to a target feature service. Defaults to False.
        include_deletes (bool, optional): a flag to include the soft-deleted records. True includes soft-deleted records. False filters-out soft-deleted records.

    Returns:
        pd.DataFrame: flattened dataframe of ncrn water results, flattened and melted to long format for formatting and

    Examples:
        import src.utils as utils
        fpath = r'data\data_export_20240628'
        mydf = bu.etl(data_folder=fpath)

        bu.dashboard_etl(data_folder=fpath, include_deletes=True)
    """
    if dest_dir != '':
        assert os.isdir(dest_dir), print(f'You provided {dest_dir=}. `dest_dir` must be an existing folder. `dashboard_etl()` will create a timestamped folder inside `dest_dir` and then save your csv there. Try again.')
    if test_run == True:
        data_folder = assets.WATER_DEV_DATA_FPATH
        target_itemid = assets.WATER_DEV_QC_DASHBOARD_BACKEND # TODO: update this to an itemid
    else:
        data_folder = assets.DM_WATER_BACKUP_FPATH
        target_itemid = assets.WATER_PROD_QC_DASHBOARD_BACKEND

    # Extract steps
    df_dict:dict = _extract(data_folder)
    
    # Transform steps
    df:pd.DataFrame = tf._transform(df_dict=df_dict, include_deletes=include_deletes)
    
    # QC checks
    tf._quality_control(df)

    # TODO:
    # if load == True:
    #     _load_feature(df, target_itemid)

    return df

def wqp_wqx(data_folder:str, include_deletes:bool=False) -> pd.DataFrame:
    # TODO: crosswalk the etl output into wqxwqp-output format
    df = pd.DataFrame()

    return df

def _extract(data_folder:str) -> dict:
    """Call-stacking function for extract steps

    Args:
        data_folder (str): relative or absolute filepath to a folder containing timestamped folders containing the .zip csv-collection downloaded from AGOL.

    Returns:
        dict: A dictionary of NCRN water monitoring data in relational form; one table in the .zip form `data_folder` becomes one key-value pair in the dictionary.
    """
    # find the newest folder in a given folder
    # use the filenames to find the newest timestamp
    dirs = [x for x in os.listdir(data_folder) if os.path.isdir(os.path.join(data_folder,x))]
    newest_data_folder = os.path.join(data_folder, max(dirs))
    assert os.path.isdir(newest_data_folder), print(f'data folder {newest_data_folder=} does not exist')

    # look at the contents of that newest folder and find a .zip file with 'csv' in the filename
    targets = os.listdir(newest_data_folder)
    targets = [x for x in targets if x.endswith('.zip') and 'csv' in x]
    assert len(targets) > 0, print(f'Returned zero csv collections in {data_folder=}')
    target = os.path.join(newest_data_folder, max(targets))

    # unzip the files
    newdir = target.rsplit('.zip',1)[0].rsplit('\\',1)[-1]
    shutil.unpack_archive(target, os.path.join(newest_data_folder, newdir))

    # extract each table
    extracted_dir = os.path.join(newest_data_folder, newdir)
    tbls = os.listdir(extracted_dir)
    tbls = [os.path.join(extracted_dir, x) for x in tbls if '_0' in x or 'ysi' in x or 'grabsample' in x]
    df_dict:dict = {}
    for tbl in tbls:
        df = pd.read_csv(tbl)
        df_dict[tbl] = df

    return df_dict

def _load_feature(df:pd.DataFrame, target_itemid:str) -> bool:

    outcome = True

    return outcome