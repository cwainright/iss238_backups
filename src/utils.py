import shutil
import os
import src.assets as srcas
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


def backup_water() -> None:
    """Generic to make backups of NCRN water source files

    Returns:
        _type_: _description_
    """

    return None

def backup_veg(src_dir:str=srcas.VEG_T_DRIVE_FPATH, dest_dir:str=srcas.DM_VEG_BACKUP_FPATH, filetypes:list=['.accdb'], verbose:bool=False) -> None:
    """Generic to make backups of NCRN forest vegetation source files

    Args:
        src_dir (str, optional): Relative or absolute filepath for the directory containing your source files. Defaults to srcas.VEG_T_DRIVE_FPATH.
        dest_dir (str, optional): Relative or absolute filepath for the directory where you want to save your backup files. Defaults to 'output'.
        filetypes (list, optional): A list of filetypes you want to copy from `src_dir`. Defaults to ['.accdb'].
        verbose (bool, optional): True turns on feedback for interactive session. Defaults to False.

    Returns:
        None

    Examples:
        import src.utils as utils

        # example 1, copy a local db (data/*.accdb) to a local backup directory (output/)

        utils.backup_veg(src_dir='data', dest_dir='output)

        # example 2, copy the T-drive (authoritative as of 2024-08-01) db to a network backup directory

        utils.backup_veg()

    """
    # check that the source directory and file exist
    assert os.path.exists(src_dir)==True, print(f'You provided {src_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert os.path.exists(dest_dir)==True, print(f'You provided {dest_dir=}, which is a directory that does not exist or is not visible to this computer. Check your filepath.')
    assert len(filetypes)>0, print(f'You provided {filetypes=}, which is an empty list. Provide one or more file extensions to copy from `src_dir` to `dest_dir`.')

    # extend `dest_dir` with a timestamp and check that that directory does not exist
    dir_ext = str(dt.datetime.now()).replace(' ','_').replace('.','_').replace(':','')
    # if dir exists, fail
    newpath = os.path.join(dest_dir, dir_ext)
    assert os.path.exists(newpath) == False, print(f'Your target directory {newpath=} already exists and this function is not allowed to overwrite existing data. The newpath is a timestamp so you can just try call the function again.')
    # if it does not exist, make dir
    os.makedirs(newpath)
    if verbose == True:
        print(f'made dir: {newpath=}')

    # copy the source file(s) from the source directory to the target directory
    filetypes = tuple(filetypes)
    source_files = []
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

def _add_log_entry(log_timestamp:str, src_file:str, log_dest:str, log_result:str, log_fpath:str=srcas.DM_BACKUP_LOG_FPATH) -> None:
    """Make an entry in the job-log for each file

    Args:
        log_timestamp (str): _description_
        src_file (str): _description_
        log_dest (str): _description_
        log_result (str): _description_
        log_fpath (str, optional): _description_. Defaults to srcas.DM_BACKUP_LOG_FPATH.

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