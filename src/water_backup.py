from arcpy.da import TableToNumPyArray, ExtendTable
import arcpy
import pandas as pd
import numpy as np
# https://github.com/vgrem/Office365-REST-Python-Client/blob/master/examples/sharepoint/files/upload.py
# https://gist.github.com/d-wasserman/e9c98be1d0caebc2935afecf0ba239a0?permalink_comment_id=3623359
def _agol_tbl_to_df(in_fc:str, input_fields:list=[], query:str="", skip_nulls:bool=False, null_values=None):
    """Convert an arcgis table into a pandas dataframe with an object ID index, and the selected input fields. Uses TableToNumPyArray to get initial data.

    https://gist.github.com/d-wasserman/e9c98be1d0caebc2935afecf0ba239a0?permalink_comment_id=3623359

    Args:
        in_fc (str): url to the table (the attribute or standalone table, not the root hosted feature layer)
        input_fields (list, optional): A list containing column names present in `in_fc`. If not empty, selects only the columns provided plus the `OID`. If empty, selects all columns. Defaults to [].
        query (str, optional): SQL query to filter records from `in_fc`. Defaults to "".
        skip_nulls (bool, optional): If True, will filter-out null rows for any column, including geometry, in `in_fc`. Defaults to False.
        null_values (list or None, optional): A list of masks for null values. E.g., a user could use a silly value like -999999 instead of np.NaN; `null_values` lets the user cover those cases. Defaults to None.

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
        
        # example 3, returns all fields and rows matching criteria in `myqry`

        myqry = "delete_grabsample = 'no' or delete_grabsample IS NULL" # note single quotes and double-quotes

        df = watb._agol_tbl_to_df(in_fc=srcas.WATER_TBL_GRABSAMPLE_URL, query=myqry)

    """
    OIDFieldName = arcpy.Describe(in_fc).OIDFieldName
    if len(input_fields)>0: # if user specifies which columns they want
        pass
    else: # if user does not specify the columns they want, return all of the columns
        exclusions = [
            'objectid'
        ]
        exclusions.extend([x.name for x in arcpy.ListFields(in_fc) if 'entry_other' in x.name])
        input_fields = [x.name for x in arcpy.ListFields(in_fc) if x.name not in exclusions]
    if OIDFieldName not in input_fields:
        final_fields = [OIDFieldName] + input_fields
    else:
        final_fields = input_fields.copy()
    np_array = arcpy.da.TableToNumPyArray(in_fc, final_fields, query, skip_nulls, null_values)
    object_id_index = np_array[OIDFieldName]
    fc_dataframe = pd.DataFrame(np_array, columns=final_fields, index=object_id_index)
    return fc_dataframe
