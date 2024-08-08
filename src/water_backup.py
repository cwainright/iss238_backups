from arcpy.da import TableToNumPyArray, ExtendTable
from arcgis.gis import GIS
import arcpy
import pandas as pd
import numpy as np
import src.assets as srcas
import os
import src.utils as utils

# https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/files/upload.py
# https://gist.github.com/d-wasserman/e9c98be1d0caebc2935afecf0ba239a0?permalink_comment_id=3623359
# https://www.youtube.com/watch?v=yLaIR7lmyqw&t=2089s

def _agol_tbl_to_df(in_fc:str, input_fields:list=[], query:str="", skip_nulls:bool=False, null_values=None) -> pd.DataFrame:
    """Convert an arcgis table into a pandas dataframe with an object ID index, and the selected input fields. Uses TableToNumPyArray to get initial data.

    https://gist.github.com/d-wasserman/e9c98be1d0caebc2935afecf0ba239a0?permalink_comment_id=3623359

    Args:
        in_fc (str): url to the table (the attribute or standalone table, not the root hosted feature layer)
        input_fields (list, optional): A list containing column names present in `in_fc`. If not empty, selects only the columns provided plus the `OID`. If empty, selects all columns. Defaults to [].
        query (str, optional): The WHERE clause of a SQL query to filter records from `in_fc`. Defaults to "".
        skip_nulls (bool, optional): If True, will filter-out null rows for any column, including geometry, in `in_fc`. Defaults to False.
        null_values (list or None, optional): A list of masks for null values. E.g., a user could use a silly value like -999999 instead of np.NaN. In that case, you'd provide a list [-999999] for `null_values` to cover those cases. Defaults to None.

    Returns:
        pd.DataFrame: The table specified above converted to a dataframe.

    Examples:
        import src.water_backup as watb
        
        import src.assets as srcas
        
        # example 1, returns all fields, all rows

        df = watb._agol_tbl_to_df(in_fc=srcas.WATER_TBL_GRABSAMPLE_URL)

        # example 2, returns fields specified in `mycols` and all rows

        mycols = ['globalid', 'parentglobalid', 'anc', 'tn', 'tp', 'lab', 'delete_grabsample']

        df = watb._agol_tbl_to_df(in_fc=srcas.WATER_TBL_GRABSAMPLE_URL, input_fields=mycols)
        
        # example 3, returns fields specified in `mycols` and rows matching criteria in `myqry`

        myqry = "delete_grabsample = 'no' or delete_grabsample IS NULL" # note single quotes and double-quotes

        df = watb._agol_tbl_to_df(in_fc=srcas.WATER_TBL_GRABSAMPLE_URL, input_fields=mycols, query=myqry)

        # example 4, query each table from the hosted feature

        df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_TBL_MAIN_URL)

        df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_TBL_YSI_URL)

        df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_TBL_GRABSAMPLE_URL)

        df = wtb._agol_tbl_to_df(in_fc=src_assets.WATER_TBL_PHOTO_URL)

    """
    # if user does not specify the columns they want, return all of the columns
    if len(input_fields)==0:
        exclusions = [
            'Shape' # the geometry of an point is returned as a 1x2 array (similar to coordinates e.g., [5.68434189e-14, 5.68434189e-14]); the returning array `np_array` must be 1-dimensional
        ]
        exclusions.extend([x.name for x in arcpy.ListFields(in_fc) if 'entry_other' in x.name]) # fields like 'entry_other_anc_bottle_size' break the arcpy.da.TableToNumPyArray() call for reasons...?
        input_fields = [x.name for x in arcpy.ListFields(in_fc) if x.name not in exclusions]
    # once we have a list of column names, we need to control for whether the user provided the `OID` (i.e., the table object index), 
    OIDFieldName = arcpy.Describe(in_fc).OIDFieldName
    if OIDFieldName not in input_fields:
        final_fields = [OIDFieldName] + input_fields
    else:
        final_fields = input_fields.copy()
    np_array = arcpy.da.TableToNumPyArray(in_fc, final_fields, query, skip_nulls, null_values)
    object_id_index = np_array[OIDFieldName]
    # handle exceptions
    try:
        fc_dataframe = pd.DataFrame(np_array, columns=final_fields, index=object_id_index)
        return fc_dataframe
    except Exception as e:
        print(f'Exception: {e}')
        print(f'{input_fields=}')
        print(f'{OIDFieldName=}')
        print(f'{np_array.shape=}')
        print(f'{final_fields=}')
        return None

def _agol_hosted_feature(newpath:str, verbose:bool, dir_ext:str, in_fc:str=srcas.WATER_AGOL_ITEM_ID, download_types:list=['CSV','File Geodatabase']) -> None:
    """Download a hosted feature layer as one or more filetypes

    Args:
        newpath (str): relative or absolute filepath to the directory where you want to save the files
        verbose (bool): Turn on or off messaging.
        dir_ext (str): The timestamp that becomes the directory name and is included in the log entry
        in_fc (str, optional): The AGOL item id for the hosted feature layer you want to download. Defaults to srcas.WATER_AGOL_ITEM_ID.
        download_types (list, optional): A list of strings. Each string is a filetype specified in 

    Returns:
        None

    """
    # connect to AGOL
    # https://www.youtube.com/watch?v=yLaIR7lmyqw&t=2089s
    try:
        gis = GIS('home') # update to user/pw 
        item = gis.content.get(in_fc)
        log_res:str = 'success'
        fname:str = f'AGOL connection to {in_fc=}'
        utils._add_log_entry(log_timestamp=dir_ext, src_file=in_fc, log_dest=fname, log_result=log_res)
    except Exception as e:
        print(f'Failed to connect to AGOL. Are you on the network?')
        log_res:str = 'fail - unable to connect to AGOL'
        fname:str = f'AGOL connection to {in_fc=}'
        utils._add_log_entry(log_timestamp=dir_ext, src_file=in_fc, log_dest=fname, log_result=log_res)

    for t in download_types:
        fname:str = f'ncrn_water_{t.lower().replace(" ","_")}_{dir_ext}'
        ftype:str = t
        fpath:str = os.path.join(newpath,fname)
        try:
            # makes a copy into your AGOL item as `ftype`, then downloads it, then deletes the item it created
            item.export(fname, export_format=ftype, parameters=None, wait=True)
            exported_item = gis.content.search(fname, ftype)
            exported_item_obj = gis.content.get(exported_item[0].itemid)
            exported_item_obj.download(save_path=newpath)
            if exported_item_obj.title != item.title and exported_item[0].itemid != in_fc and exported_item_obj.itemid != in_fc: # nuke prod make dev sad...
                exported_item_obj.delete(dry_run=False)
                log_res:str = 'success'
                utils._add_log_entry(log_timestamp=dir_ext, src_file=in_fc, log_dest=fpath, log_result=log_res)
                if verbose == True:
                    print(f'Saved asset {fpath}')
        except Exception as e:
            print(f'Failed to download AGOL content.')
            log_res:str = f'fail - unable to export {ftype} AGOL item {in_fc=}'
            utils._add_log_entry(log_timestamp=dir_ext, src_file=in_fc, log_dest=fpath, log_result=log_res)

    return None

def _download_csvs(newpath:str, verbose:bool, dir_ext:str) -> None:
    """Download a csv of each table in a dictionary of table names and AGOL urls

    Args:
        newpath (str): relative or absolute filepath to the directory where you want to save the files
        verbose (bool): Turn on or off messaging
        dir_ext (str): The timestamp that becomes the directory name and is included in the log entry

    Returns:
        None

    """

    for k,v in srcas.WATER_AGOL_ASSETS.items():
        df:pd.DataFrame = _agol_tbl_to_df(in_fc=v)
        fname:str = os.path.join(newpath, k + '.csv')
        try:
            df.to_csv(fname, index=False)
            log_res:str='success'
            utils._add_log_entry(log_timestamp=dir_ext, src_file=v, log_dest=os.path.join(newpath, fname.rsplit('\\',1)[1]), log_result=log_res)
            if verbose == True:
                print(f'Queried tbl {k=} from source...')
                print(f'Wrote csv: {fname=}')
        except Exception as e:
                print(e)
                log_res = 'fail'
                utils._add_log_entry(log_timestamp=dir_ext, src_file=v, log_dest=os.path.join(newpath, fname.rsplit('\\',1)[1]), log_result=log_res)

    return None
