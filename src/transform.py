"""ETL pipeline extract steps for NCRN discrete water quality monitoring"""

import pandas as pd
import numpy as np
import src.assets as assets

def _transform(df_dict:dict, include_deletes:bool) -> pd.DataFrame:

    tfm_site_visits = _transform_site_visits(tbl=df_dict['tbl_main']['df'].copy(), include_deletes=include_deletes) # site-visit level data (location, datetime, field crew names, etc.)
    tfm_tbl_main = _transform_tbl_main(tbl_main=df_dict['tbl_main']['df'].copy(), include_deletes=include_deletes) # fields that are one-to-one with site visits
    tfm_tbl_ysi = _transform_tbl_ysi(tbl_ysi=df_dict['tbl_ysi']['df'].copy(), include_deletes=include_deletes) # ysi fields that are zero or one-to-many with site visits
    tfm_tbl_grabsample = _transform_tbl_grabsample(tbl_grabsample=df_dict['tbl_grabsample']['df'].copy(), include_deletes=include_deletes) # grabsample fields that are zero or one-to-many with site visits

    df = pd.concat([tfm_tbl_main, tfm_tbl_ysi, tfm_tbl_grabsample])
    df = pd.merge(tfm_site_visits, df, left_on='SiteVisitGlobalID', right_on='ParentGlobalID')

    # clean
    excludes = ['x','y','entry_anc_btl_size','entry_other_nutrient_btl_size','entry_other_anc_btl_size','entry_nutrient_btl_size','entry_algae_description', 'entry_other_algae_description','entry_q_instrument','entry_other_q_instrument','entry_ysi_probe','entry_other_ysi_probe','entry_ysi_increment','entry_other_ysi_increment','entry_other_lab','entry_lab','duplicate_collected']
    # excludes.extend([x for x in df.Characteristic_Name if 'flag'])
    mask = (df['Characteristic_Name'].isin(excludes)==False) & (df['Characteristic_Name'].isin(assets.SITE_VISIT_COLS)==False) & (df['Characteristic_Name'].str.contains('delete')==False) & (df['Characteristic_Name'].str.contains('flag')==False)
    df = df[mask]
    df.reset_index(drop=True, inplace=True)

    df = _apply_data_types(df)
    df = _decode(df)
    del_these = ['reviewer_name','other_reviewer','entry_field_crew','entry_other_field_crew']
    for c in del_these:
        del df[c]
    df = _gather_others(df)
    df = _scrub_locs(df)
    df = _cast_result_by_type(df)
    df = _calc_week_of_year(df)

    df_a = df[df['num_result'].isna()==False].head(len(df)).copy()
    df_b = df[df['num_result'].isna()].head(len(df)).copy()
    df = pd.concat([df_a,df_b])
    df = df[df['data_quality_flag']!='permanently_missing']
    df = _soft_constraints(df)
    mask = (df['Characteristic_Name'].str.contains('_notes')==False)
    df = df[mask]
    ignores = ['anc_method','landuse_category','dom_riparian_ter_veg_sp','channelized','bank_stability','entry_stream_phy_appear']
    mask = (df['Characteristic_Name'].isin(ignores)==False)
    df = df[mask]
    df = df.reset_index(drop=True)

    # assign activity_id
    # df = _assign_activity_id(df=df) # this only makes sense for verified records

    return df

def _assign_activity_id(df:pd.DataFrame) -> pd.DataFrame:

    # fail if any characteristics/results do not have a `grouping_var` assigned
    problems = df[df['grouping_var'].isna()]
    if len(problems) > 0:
        for x in problems.Characteristic_Name.unique():
            print(f'ERROR: `Characteristic_Name` has not been assigned a `grouping_var`: {x}')
    assert (len(problems)) == 0, print(f'{len(problems)} rows in the dataframe have not been assigned a `grouping_var`')

    # fail if any characteristics/results do not have a `activity_group_id` assigned
    problems = df[df['activity_group_id'].isna()]
    if len(problems) > 0:
        for x in problems.activity_group_id.unique():
            print(f'ERROR: `activity_group_id` has not been assigned: {x}')
    assert (len(problems)) == 0, print(f'{len(problems)} rows in the dataframe have not been assigned a `activity_group_id`')

    LOOKUP = [
        # hard-coding `activity_id` based on `grouping_var` fails when the values present in `grouping_var` are different than expected
        # so we have to check that the values present in the dataframe are equal to the ones we expect before assigning `activity_id`
        'NCRN_WQ_HABINV'
        ,'NCRN_WQ_WQUANTITY'
        ,'NCRN_WQ_WQUALITY'
        ,'NCRN_WQ_WCHEM'
    ]
    # check that values present in the df are present in the lookup
    problems = []
    for x in df.grouping_var.unique():
        if x not in LOOKUP:
            problems.append(x)
    assert len(problems)==0, print(f'{len(problems)} `grouping_var` values are present in the dataframe but absent from `_assign_activity_id().LOOKUP`:\n\n{problems}')
    # check that values present in the lookup are present in the df
    problems = []
    for x in LOOKUP:
        if x not in df.grouping_var.unique():
            problems.append(x)
    assert len(problems)==0, print(f'{len(problems)} `grouping_var` values are present in `_assign_activity_id().LOOKUP` but absent from the dataframe:\n\n{problems}')

    # if we pass all of the above checks, assign the `activity_id`
    df['activity_id'] = df['activity_group_id']+'|'+df['grouping_var'] # base case

    mask = (df['grouping_var']=='NCRN_WQ_HABINV') # i.e., site observations
    df['activity_id'] = np.where(mask, df['activity_group_id']+'|'+df['grouping_var'], df['activity_id'])

    mask = (df['grouping_var']=='NCRN_WQ_WQUANTITY') # i.e., flowtracker characteristics
    df['activity_id'] = np.where(mask, df['activity_group_id']+'|'+df['grouping_var'], df['activity_id'])

    mask = (df['grouping_var']=='NCRN_WQ_WQUALITY') & (df['sampleability']=='Actively Sampled') & (df['ysi_probe'].isna()==False) & (df['ysi_increment'].isna()==False) # i.e., ysi characteristics
    df['activity_id'] = np.where(mask, df['activity_group_id']+'|'+df['grouping_var']+'|'+df['ysi_probe']+'|'+df['ysi_increment'], df['activity_id'])

    mask = (df['grouping_var']=='NCRN_WQ_WCHEM') & (df['sampleability']=='Actively Sampled') & (df['lab'].isna()==False) # i.e., lab results
    df['activity_id'] = np.where(mask, df['activity_group_id']+'|'+df['grouping_var']+'|'+df['lab'], df['activity_id'])

    # every row in df should receive an `activity_id` without any exceptions
    # `activity_id` resolves to None when any part of the concatenation is None
    # any None value is a problem, and the pipeline should stop
    # None values usually come from things that were left blank in the Survey that shouldn't be blank (e.g., overriding)
    problems = df[df['activity_id'].isna()]
    if len(problems) > 0:
        for x in problems.grouping_var.unique():
            print(f'ERROR: `grouping_var` failed to assign an `activity_id`: {x}')
    assert len(problems)==0, print(f'{len(problems)} `activity_id` failed to assign in {len(problems)} rows`. Resolve this problem by fixing the `activity_id` assignment logic in `transform._assign_activity_id()`')

    return df

def _soft_constraints(df:pd.DataFrame) -> pd.DataFrame:
    """add a warning message for results that exceed soft-constraints

    This becomes a summary table in the QC dashboard

    Args:
        df (pd.DataFrame): flattened dataframe of NCRN water results

    Returns:
        pd.DataFrame: flattened dataframe of NCRN water results with flags added
    """

    df['year'] = df['activity_start_date'].str.split(pat='-',n=1).str[0]
    df['month'] = df['activity_start_date'].str.split(pat='-').str[1]
    df['key'] = df['location_id']+df['year']+df['month']+df['Characteristic_Name']

    constraints = pd.read_csv(assets.SOFT_CONSTRAINTS)
    constraints.rename(columns={
        'Location_ID':'location_id'
        ,'Month':'month'
        ,'Year':'year'
    },inplace=True)
    tmp = constraints.melt(id_vars=['key','location_id','month','year'])
    lows = tmp[tmp['variable'].str.contains('low_')].copy().reset_index(drop=True)
    highs = tmp[tmp['variable'].str.contains('low_')==False].copy().reset_index(drop=True)
    lows['variable'] = lows['variable'].str.replace('low_','')
    highs['variable'] = highs['variable'].str.replace('high_','')
    lows['key'] = lows['key']+lows['variable']
    highs['key'] = highs['key']+highs['variable']
    lows.rename(columns={
        'value':'low'
    },inplace=True)
    highs.rename(columns={
        'value':'high'
    },inplace=True)
    constraints = pd.merge(highs[['key','high']],lows[['key','low']], on='key')

    df = pd.merge(df,constraints, on='key', how='left')
    del df['key']
    del df['year']
    del df['month']
    df['result_warning'] = None
    mask = (df['data_type']=='float') & (df['num_result']<=df['low']) & (df['review_status']!='verified')
    df['result_warning'] = np.where(mask, f'result is below soft constraint', df['result_warning'])
    # df[mask][['data_type','Characteristic_Name','activity_group_id','num_result','low','result_warning']] # sanity check
    mask = (df['data_type']=='float') & (df['num_result']>=df['high']) & (df['review_status']!='verified')
    df['result_warning'] = np.where(mask, f'result is above soft constraint', df['result_warning'])
    # df[mask][['data_type','Characteristic_Name','activity_group_id','num_result','high','result_warning']] # sanity check

    return df

def _calc_week_of_year(df:pd.DataFrame) -> pd.DataFrame:
    df['week_of_year'] = pd.to_datetime(df['activity_start_date']).dt.isocalendar().week
    return df

def _transform_site_visits(tbl:pd.DataFrame, include_deletes:bool) -> pd.DataFrame:

    site_visits = tbl[assets.SITE_VISIT_COLS].copy()
    site_visits.rename(columns={'GlobalID':'SiteVisitGlobalID'}, inplace=True)

    # filter out soft-deleted records
    if include_deletes==False:
        site_visits = _remove_deletes(site_visits)

    return site_visits

def _transform_tbl_ysi(tbl_ysi:pd.DataFrame, include_deletes:bool) -> pd.DataFrame:

    # subset the columns
    MAIN_COLS = [
        'GlobalID'
    ]
    ID_COLS = MAIN_COLS.copy()
    excludes = ['objectid', 'globalid', 'parentglobalid']
    excludes.extend(assets.SITE_VISIT_COLS)
    adds = [x for x in tbl_ysi.columns if x.lower() not in excludes]
    MAIN_COLS.extend(adds)
    ID_COLS.extend(['ysi_increment','ysi_probe'])
    lookup = tbl_ysi[['GlobalID','ParentGlobalID']]

    # filter out soft-deleted records
    if include_deletes==False:
        tbl_ysi = _remove_deletes(tbl_ysi)
    tbl_ysi = tbl_ysi[MAIN_COLS]

    # melt
    VALUE_COLS = [x for x in tbl_ysi.columns if x not in ID_COLS]
    tfm_tbl_ysi = tbl_ysi.melt(id_vars=ID_COLS, value_vars=VALUE_COLS, var_name='Characteristic_Name',value_name='Result_Text')
    tfm_tbl_ysi = pd.merge(tfm_tbl_ysi, lookup, on='GlobalID')
    tfm_tbl_ysi['grouping_var'] = 'NCRN_WQ_WQUALITY'
    for c in assets.FLAT_COLS:
        if c not in tfm_tbl_ysi.columns:
            tfm_tbl_ysi[c] = None
    tfm_tbl_ysi = tfm_tbl_ysi[assets.FLAT_COLS]
    tfm_tbl_ysi = _apply_data_flags(tfm_tbl_ysi, tbl_ysi)

    return tfm_tbl_ysi

def _transform_tbl_main(tbl_main:pd.DataFrame, include_deletes:bool) -> pd.DataFrame:

    # subset the columns
    MAIN_COLS = [
        'GlobalID'
    ]
    ID_COLS = MAIN_COLS.copy()
    adds = [x for x in tbl_main.columns if x.lower() != 'objectid' and x not in assets.SITE_VISIT_COLS]
    MAIN_COLS.extend(adds)

    # filter out soft-deleted records
    if include_deletes==False:
        tbl_main = _remove_deletes(tbl_main)
    tbl_main = tbl_main[MAIN_COLS]

    # melt
    VALUE_COLS = [x for x in tbl_main.columns if x not in ID_COLS]
    tfm_tbl_main = tbl_main.melt(id_vars=ID_COLS, value_vars=VALUE_COLS, var_name='Characteristic_Name',value_name='Result_Text')
    tfm_tbl_main['ParentGlobalID'] = tfm_tbl_main['GlobalID']
    tfm_tbl_main['grouping_var'] = None

    # add the grouping variable
    lu = {
        'NCRN_WQ_HABINV':[
            'air_temperature'
            ,'algae_cover_percent'
            ,'algae_description'
            ,'stream_physical_appearance'
            ,'flow_status'
            ,'sampleability'
            ,'visit_type'
            ,'rain_last_24'
            ,'photos_y_n'
            ,'weather_condition'
            ,'left_bank_riparian_width'
            ,'right_bank_riparian_width'
            ,'entry_other_stream_phy_appear'
        ]
        ,'NCRN_WQ_WQUANTITY':[
            'discharge'
            ,'mean_velocity'
            ,'mean_crossection_depth'
            ,'flowtracker_notes'
            ,'wetted_width'
            ,'tape_offset'
            ,'discharge_instrument'
        ]
    }
    for c in tfm_tbl_main.Characteristic_Name.unique():
        for k,v in lu.items():
            if c in v:
                mask = (tfm_tbl_main['Characteristic_Name']==c)
                tfm_tbl_main['grouping_var'] = np.where(mask, k, tfm_tbl_main['grouping_var'])

    for c in assets.FLAT_COLS:
        if c not in tfm_tbl_main.columns:
            tfm_tbl_main[c] = None
    tfm_tbl_main = tfm_tbl_main[assets.FLAT_COLS]
    tfm_tbl_main = _apply_data_flags(tfm_tbl_main, tbl_main)

    return tfm_tbl_main

def _transform_tbl_grabsample(tbl_grabsample:pd.DataFrame, include_deletes:bool) -> pd.DataFrame:

    # subset the columns
    MAIN_COLS = [
        'GlobalID'
    ]
    ID_COLS = MAIN_COLS.copy()
    excludes = ['objectid', 'globalid', 'parentglobalid']
    excludes.extend(assets.SITE_VISIT_COLS)
    adds = [x for x in tbl_grabsample.columns if x.lower() not in excludes]
    MAIN_COLS.extend(adds)
    ID_COLS.extend(['lab','anc_method'])
    lookup = tbl_grabsample[['GlobalID','ParentGlobalID']]
    
    # filter out soft-deleted records
    if include_deletes==False:
        tbl_grabsample = _remove_deletes(tbl_grabsample)
    tbl_grabsample = tbl_grabsample[MAIN_COLS]

    # melt
    VALUE_COLS = [x for x in tbl_grabsample.columns if x not in ID_COLS]
    tfm_tbl_grabsample = tbl_grabsample.melt(id_vars=ID_COLS, value_vars=VALUE_COLS, var_name='Characteristic_Name',value_name='Result_Text')
    tfm_tbl_grabsample = pd.merge(tfm_tbl_grabsample, lookup, on='GlobalID')
    tfm_tbl_grabsample['grouping_var'] = 'NCRN_WQ_WCHEM'
    for c in assets.FLAT_COLS:
        if c not in tfm_tbl_grabsample.columns:
            tfm_tbl_grabsample[c] = None
    tfm_tbl_grabsample = tfm_tbl_grabsample[assets.FLAT_COLS]
    mask = (tfm_tbl_grabsample['lab']=='CUE') & (tfm_tbl_grabsample['Characteristic_Name']=='anc')
    tfm_tbl_grabsample['anc_method'] = np.where(mask, tfm_tbl_grabsample['anc_method'], None)
    tfm_tbl_grabsample = _apply_data_flags(tfm_tbl_grabsample, tbl_grabsample)

    return tfm_tbl_grabsample

def _apply_data_flags(tfm_tbl:pd.DataFrame, tbl:pd.DataFrame) -> pd.DataFrame:
    """group and apply the data quality flags for each result"""

    # subset the columns
    MAIN_COLS = [
        'GlobalID'
    ]
    ID_COLS = MAIN_COLS.copy()
    flags = [x for x in tbl.columns if 'flag' in x.lower()]
    adds = [x for x in tbl.columns if x.lower() != 'objectid' and x not in assets.SITE_VISIT_COLS and x not in flags]
    flags.extend(['GlobalID'])
    MAIN_COLS.extend(adds)

    # make a lookup table of data quality flags
    # the flags are in wide-format and each characteristic has two relevant columns: 'flag' column and an 'other' column
    # this design is required so the user can select "other" in the picklist, and then type some other flag into a text box as needed
    # we only care about the flag "result", so when the user chose "other" as their flag, we need to move their input to the 'flag' column
    flags_lookup = tbl[flags].copy()
    flags_xwalk = {}
    for flag in flags:
        char = flag.rsplit('_',1)[0]
        if char not in flags_xwalk.keys() and char != 'GlobalID':
            flags_xwalk[flag] = char

    abbreviations = { # a crosswalk to overcome me having to abbreviate some column names in S123
        'mean_crossection_depth_flag': 'other_mean_crossection_dep_flag'
        ,'ysi_increment_distance_flag': 'other_ysi_increment_dist_flag'
    }
    for col in flags_lookup.columns:
        # figure out each column's "other" equivalent
        if col in abbreviations.keys():
            othercol = abbreviations[col]
        else:
            othercol = f"other_{col}"
        # print(othercol)

        # update the non-"other" flag column to the "other" flag when user enters 'other' as their flag and then uses the text box to enter another value
        excludes = [
            'GlobalID'
            ]
        if 'other' not in col and col not in excludes:
            try:
                mask = (flags_lookup[col].astype(str).str.lower().str.contains('other')) & (flags_lookup[col].isna()==False)
                flags_lookup[col] = np.where(mask, flags_lookup[othercol], flags_lookup[col])
            except:
                print(f"FAIL: {col=}: {othercol=}") # sanity check; is the lookup table of "other" columns complete?
                break
    for col in flags_lookup.columns:
        if 'other' in col:
            del flags_lookup[col]

    # standardize the lookup table to include the characteristic names present in the long-format dataset
    flags_lookup.rename(columns=flags_xwalk, inplace=True)
    idcols = ['GlobalID']
    varcols = [x for x in flags_lookup if x not in idcols]
    flags_lookup = pd.melt(flags_lookup, id_vars=idcols, value_vars=varcols, value_name='data_quality_flag', var_name='Characteristic_Name')
    # flags_lookup[flags_lookup['data_quality_flag'].isna()==False] # sanity check

    # join the lookup table to the long-format dataset
    tfm_tbl = pd.merge(tfm_tbl, flags_lookup, how='left', on=['GlobalID','Characteristic_Name'])
    tfm_tbl.data_quality_flag.unique()
    # tfm_tbl[tfm_tbl['data_quality_flag'].isna()==False] # sanity check

    return tfm_tbl

def _decode(tfl_tbl:pd.DataFrame) -> pd.DataFrame:
    lookup = pd.read_excel(assets.CONTACTS, 'choices')
    
    # tfl_tbl.record_reviewers
    # tfl_tbl.field_crew
    tfl_tbl = _decode_names(tfl_tbl, lookup)

    # tfl_tbl.Characteristic_Name.weather_condition
    # tfl_tbl.Characteristic_Name.ysi_probe
    tfl_tbl = _decode_chars(tfl_tbl, lookup)

    return tfl_tbl

def _apply_data_types(tfm_tbl:pd.DataFrame) -> pd.DataFrame:

    mycols = ['skip_req_observations', 'skip_req_observations_reason',
       'air_temperature', 'weather_condition', 'rain_last_24',
       'algae_cover_percent', 'algae_description',
       'entry_stream_phy_appear', 'entry_other_stream_phy_appear',
       'flow_status', 'left_bank_riparian_width',
       'right_bank_riparian_width', 'landuse_category',
       'dom_riparian_ter_veg_sp', 'channelized', 'bank_stability',
       'site_observation_notes', 'skip_req_ysi', 'skip_req_ysi_reason',
       'skip_req_flowtracker', 'skip_req_flowtracker_reason',
       'discharge_instrument', 'wetted_width', 'discharge',
       'mean_velocity', 'mean_crossection_depth', 'flowtracker_notes',
       'skip_req_grabsample', 'skip_req_grabsample_reason',
       'grabsample_notes', 'skip_req_photo', 'skip_req_photo_reason',
       'ysi_probe', 'ysi_increment', 'ysi_increment_distance',
       'water_temperature', 'barometric_pressure', 'conductivity',
       'specific_conductance', 'turbidity', 'salinity', 'ph',
       'do_concentration', 'do_saturation', 'tds', 'ysi_increment_notes',
       'duplicate_y_n', 'lab', 'anc_bottle_size', 'nutrient_bottle_size',
       'anc', 'tn', 'tp', 'ammonia', 'orthophosphate', 'chlorine',
       'nitrate','anc_method']

    # for x in mycols:
    #     print(f"lookup['{x}'] = ['',None]")

    lookup = {}
    for char in mycols:
        lookup[char] = None

    lookup['skip_req_observations'] = ['bool', None]
    lookup['skip_req_observations_reason'] = ['string',None]
    lookup['air_temperature'] = ['float', 'deg C']
    lookup['weather_condition'] = ['string',None]
    lookup['rain_last_24'] =  ['bool', None]
    lookup['algae_cover_percent'] = ['string',None]
    lookup['algae_description'] = ['string',None]
    lookup['entry_stream_phy_appear'] = ['string',None]
    lookup['entry_other_stream_phy_appear'] = ['string',None]
    lookup['flow_status'] = ['string',None]
    lookup['left_bank_riparian_width'] = ['float','m']
    lookup['right_bank_riparian_width'] = ['float','m']
    lookup['landuse_category'] = ['string',None]
    lookup['dom_riparian_ter_veg_sp'] = ['string',None]
    lookup['channelized'] = ['string',None]
    lookup['bank_stability'] = ['string',None]
    lookup['site_observation_notes'] = ['string',None]
    lookup['skip_req_ysi'] = ['string',None]
    lookup['skip_req_ysi_reason'] = ['string',None]
    lookup['skip_req_flowtracker'] = ['string',None]
    lookup['skip_req_flowtracker_reason'] = ['string',None]
    lookup['discharge_instrument'] = ['string',None]
    lookup['wetted_width'] = ['float', 'ft']
    lookup['discharge'] = ['float', 'cfs']
    lookup['mean_velocity'] = ['float', 'ft/s']
    lookup['mean_crossection_depth'] = ['float','ft']
    lookup['flowtracker_notes'] = ['string',None]
    lookup['skip_req_grabsample'] = ['string',None]
    lookup['skip_req_grabsample_reason'] = ['string',None]
    lookup['grabsample_notes'] = ['string',None]
    lookup['skip_req_photo'] = ['string',None]
    lookup['skip_req_photo_reason'] = ['string',None]
    lookup['ysi_probe'] = ['string',None]
    lookup['ysi_increment'] = ['string',None]
    lookup['ysi_increment_distance'] = ['float','ft']
    lookup['water_temperature'] = ['float','deg C']
    lookup['barometric_pressure'] = ['float', 'mm Hg']
    lookup['conductivity'] = ['float', 'uS/cm']
    lookup['specific_conductance'] = ['float', 'uS/cm']
    lookup['turbidity'] = ['float', 'ntu']
    lookup['salinity'] = ['float', 'ppt']
    lookup['ph'] = ['float', 'pH']
    lookup['do_concentration'] = ['float', 'mg/L']
    lookup['do_saturation'] = ['float', 'percent']
    lookup['tds'] = ['float','mg/L']
    lookup['ysi_increment_notes'] = ['string',None]
    lookup['duplicate_y_n'] = ['string',None]
    lookup['lab'] = ['string',None]
    lookup['anc_bottle_size'] = ['string',None]
    lookup['nutrient_bottle_size'] = ['string',None]
    lookup['anc'] = ['float','ueq/L']
    lookup['tn'] = ['float','mg/L']
    lookup['tp'] = ['float','mg/L']
    lookup['ammonia'] = ['float','mg/L']
    lookup['orthophosphate'] = ['float','mg/L']
    lookup['chlorine'] = ['float','mg/L']
    lookup['nitrate'] = ['float','mg/L']
    lookup['tdp'] = ['float','mg/L']
    lookup['tdn'] = ['float','mg/L']
    lookup['anc_method'] = ['string',None]
    lookup['stream_physical_appearance'] = ['string',None]
    lookup['tape_offset'] = ['float','ft']

    # sanity checks so we keep the lookup table up-to-date
    for char in tfm_tbl.Characteristic_Name:
        if char not in lookup.keys():
            print(f"WARNING: `Characteristic_Name` {char=} is missing from your lookup table in `src.transform._apply_data_types()`")
            print(f"to resolve this warning, add an entry in the lookup table for {char}")

    for k,v in lookup.items():
        if v is None or not v:
            print(f"WARNING: missing value in key value pair key-value pair")
            print(f"    {k=}: {v=}")

    # apply the data types per the lookup table
    tfm_tbl['data_type'] = None # base case
    tfm_tbl['Result_Unit'] = None
    for k,v in lookup.items(): # case-statements
        mask = (tfm_tbl['Characteristic_Name']==k)
        tfm_tbl['data_type'] = np.where(mask, v[0], tfm_tbl['data_type'])
        tfm_tbl['Result_Unit'] = np.where(mask, v[1], tfm_tbl['Result_Unit'])

    return tfm_tbl

def _remove_deletes(tbl:pd.DataFrame) -> pd.DataFrame:
    del_col = [x for x in tbl.columns if 'delete' in x]
    assert len(del_col)==1, print(f"FAIL: too many delete columns {del_col=}")
    mask = (tbl[del_col[0]].str.lower() != 'yes')
    tbl = tbl[mask]
    tbl.reset_index(drop=True, inplace=True)
    return tbl

def _decode_names(tfl_tbl:pd.DataFrame, lookup:pd.DataFrame) -> pd.DataFrame:
    """Decode people's names to human-readable format based on lookup table"""
    name_permutations = [] # sort through the permutations of combinations of names to minimize the number of times we look up each name
    name_permutations.extend(tfl_tbl.record_reviewers.unique())
    name_permutations.extend(tfl_tbl.field_crew.unique())
    name_permutations = list(set(name_permutations)) # deduplicate
    name_permutations = [x for x in name_permutations if pd.isna(x)==False] # remove NAs
    name_vals = [x.replace('other','').replace(',,',',').strip().split(',') for x in name_permutations] # clean
    name_lookup = {k:{'individuals':v,'name_str':None} for (k,v) in zip(name_permutations,name_vals)} # map to data structure
    finds = ['reviewers','field_crew']
    mask = (lookup['list_name'].isin(finds))
    choices = lookup[mask].drop_duplicates('name')

    for k,v in name_lookup.items():
        n_str = []
        for n in v['individuals']:
            n = n.strip()
            try: # if the name is in the lookup, it'll pass this try block
                n_full = choices[choices['name']==n].label.unique()[0]
                # print(n_full)
            except: # if the name is not in the lookup, it'll come to the except block
                n = n.split(' ') # the name could be one or more words
                if len(n) >1: # if the name is more than one word, obfuscate words 2...n (e.g., "John Jack Doe" should be come "John J. D.")
                    n_obfuscate = []
                    for i in range(len(n)):
                        if i ==0:
                            n_obfuscate.append(n[i])
                        else:
                            part = f"{n[i][:1]}."
                            n_obfuscate.append(part)
                    n_full = ' '.join(n_obfuscate)
                else: # if the name is only one word, keep it
                    n_full = n[0]
                # print(f"{n_full=}")
            n_str.append(n_full)
        n_display = ', '.join(n_str)
        name_lookup[k]['name_str'] = n_display

    for k,v in name_lookup.items():
        mask = (tfl_tbl['record_reviewers']==k)
        tfl_tbl['record_reviewers'] = np.where(mask, v['name_str'], tfl_tbl['record_reviewers'])
        mask = (tfl_tbl['field_crew']==k)
        tfl_tbl['field_crew'] = np.where(mask, v['name_str'], tfl_tbl['field_crew'])
    
    return tfl_tbl

def _decode_chars(tfl_tbl:pd.DataFrame, lookup:pd.DataFrame) -> pd.DataFrame:
    """Decode characteristics to human-readable format based on lookup table"""
    chars = {'weather_condition':'weather_condition', 'ysi_probes':'ysi_probe'} # account for discrepancy between lookup charname and colname...
    # key = the name of the lookup table `list_name`
    # value = the name of the characteristic in `tfl_tbl` to which the `k` refers
    for a,b in chars.items():
        lu = lookup[lookup['list_name']==a].copy()
        if a == 'weather_condition':
            lu['name'] = lu['name'].astype(int) # type-cast because py interprets 0 and 1 as `bool` for reasons
        lu = {k:v for (k,v) in zip(lu['name'].values, lu['label'].values)}
        for k,v in lu.items():
            mask = (tfl_tbl['Characteristic_Name']==b) & (tfl_tbl['Result_Text']==k)
            tfl_tbl['Result_Text'] = np.where(mask, v, tfl_tbl['Result_Text'])
    
    return tfl_tbl

def _scrub_locs(tfl_tbl:pd.DataFrame) -> pd.DataFrame:
    lu = assets.LOCS

    # update mydf.ncrn_latitude and mydf.ncrn_longitude per the lookup table
    for k,v in lu.items():
        mask = (tfl_tbl['location_id']==k)
        tfl_tbl['ncrn_latitude'] = np.where(mask, v[2], tfl_tbl['ncrn_latitude'])
        tfl_tbl['ncrn_longitude'] = np.where(mask, v[3], tfl_tbl['ncrn_longitude'])
        tfl_tbl['ncrn_site_name'] = np.where(mask, v[1], tfl_tbl['ncrn_site_name'])

    return tfl_tbl

def _gather_others(tfl_tbl:pd.DataFrame) -> pd.DataFrame:

    # columns should come in threes when there is an "other" option in the picklist
    # e.g., ['entry_field_crew', 'entry_other_field_crew', 'field_crew']
    # this gives the user the ability to choose "other" in the picklist and then type a non-picklist choice while afield
    # e.g., if some new person goes with the regular field crew, the user needs to record that new person's name somehow
    # this function combines all three columns

    # names are already dealt-with in `_decode_names()`

    # find the columns that have 'other' in their name
    # others = [x for x in tfl_tbl.columns if 'other' in x and 'location' not in x] # we deal with locations in `src.transform._scrub_locs()`
    others = [x for x in tfl_tbl.columns if 'other' in x]
    entry = [x.replace('other_','') for x in others]
    root = [x.replace('entry_','') for x in entry]

    mismatches = [x for x in entry if x not in tfl_tbl.columns]
    mismatches.extend([x for x in root if x not in tfl_tbl.columns])
    entries = [x for x in entry if x not in mismatches]
    roots = [x for x in root if x not in mismatches]
    if len(mismatches)!=0:
        for m in mismatches:
            print(f"WARNING: failed to parse `other` column: {m}. See `src.transform._gather_others()`")

    lu = {}
    for i in range(len(others)):
        if others[i] == 'other_location_name':
            r = 'ncrn_site_name' # exception
        else:
            r = roots[i]
        lu[others[i]] = {'entry':entries[i], 'other':others[i], 'root':r}

    for k,v in lu.items():
        e = v['entry']
        r = v['root']
        mask = (tfl_tbl[e].str.contains('other'))
        tfl_tbl[r] = np.where(mask, tfl_tbl[k], tfl_tbl[e])
        del tfl_tbl[k]
        del tfl_tbl[e]

    return tfl_tbl

def _cast_result_by_type(tfl_tbl:pd.DataFrame) -> pd.DataFrame:

    tfl_tbl['num_result'] = np.NaN
    tfl_tbl['str_result'] = None

    numtypes = ['float'] # a list of the number types designated in `tfl_tbl.data_type`, in case we need additional data types in the future
    mask = (tfl_tbl['data_type'].isin(numtypes))
    tfl_tbl['num_result'] = np.where(mask, tfl_tbl['Result_Text'], tfl_tbl['num_result'])
    # tfl_tbl['num_result'] = tfl_tbl['num_result'].astype(float)

    texttypes = ['bool', 'string'] # a list of non-numeric types that we want to include as group-by and counts in the dashboard
    mask = (tfl_tbl['data_type'].isin(texttypes))
    tfl_tbl['str_result'] = np.where(mask, tfl_tbl['Result_Text'], tfl_tbl['str_result'])

    return tfl_tbl

def _quality_control(df:pd.DataFrame) -> pd.DataFrame:
    """Enforce business logic to quality-control the output of the pipeline"""

    # hard-code project name
    df['project_id'] = 'Perennial stream water monitoring'

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
            print("--------------------------------------------------------------------------------")
            print(f'WARNING (a): non-nullable field `{c}` is null in all rows')
            print("")

    # business rule-checking logic
    # if `review_status` IN ['verified', 'in_review'], the following fields are non-nullable

    statuses = ['verified']
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
        problems = df[mask]
        if len(problems) >0:
                print("--------------------------------------------------------------------------------")
                print(f'WARNING (b): non-nullable field `{c}` is null in {round(((len(problems))/len(df)*100),2)}% of rows of verified records')
                print('Resolve these warnings by assigning a value to this column\nE.g.,')
                try:
                    mycols = ['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag']
                    mycols.append(c)
                    mask = (problems['review_status'].isin(statuses)) & (problems[c].isna()) & (problems['delete_record'].isna()==False) & (problems['delete_record']!='yes')
                    print(f'Printing head(2); {len(problems[mask])} rows from {len(problems.activity_group_id.unique())} site visits match these criteria')
                    for x in problems.activity_group_id.unique()[:2]:
                        mask = (problems['activity_group_id']==x)
                        print(problems[mask][mycols].head(2))
                except:
                    print(f'Failed to print warnings for {c}')

    # # if `data_type` == 'float', the following fields are non-nullable
    #     [
    #         'Result_Unit'
    #     ]
    non_nullables = ['Result_Unit']
    for c in non_nullables:
        mask = (df['data_type']=='float') & (df[c].isna())
        problems = df[mask]
        if len(problems) > 0:
                print("--------------------------------------------------------------------------------")
                print(f'WARNING (c): non-nullable field `{c}` is null in {round(((len(problems))/len(df)*100),2)}% of rows')
                print("")
                for x in problems.activity_group_id.unique()[:2]:
                    mask = (problems['activity_group_id']==x)
                    print(problems[mask][['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag']])

    # check for duplicate `activity_group_id`s for each `SiteVisitParentGlobalID`
    # group by `activity_group_id` and count `SiteVisitParentGlobalID`
    # filter to count>1
    # print those warnings
    problems = df.copy().drop_duplicates('SiteVisitGlobalID')[['activity_group_id','SiteVisitGlobalID']].groupby(['activity_group_id','SiteVisitGlobalID']).size().reset_index(name='n').sort_values(['n'], ascending=True)
    problems = problems[problems['n']>1] # anything in this subset is a duplicated site visit record in S123
    if len(problems) > 0:
        print("--------------------------------------------------------------------------------")
        print(f'WARNING: {len(problems)} site visits are duplicated in S123\nResolve these warnings by deduplicating the site visits in S123\nE.g.,')
        if len(problems) < 100:
            print('Printing all duplicated site visits:')
            print(problems.to_string())
        else:
            print('More than 100 site visits are duplicated.\nPrinting the first 100...')
    else:
        print('There are no duplicated site visits!')

    statuses = ['verified']
    # are our data quality flags uniform?
    # did we ever report 0 (or a negative number) as the result for nutrients? TN, TP, ammonia, etc. probably should change those to NA and update their flag to p<QL
    mask = (df['review_status'].isin(statuses)) & (df['num_result']<=0) & (df['data_quality_flag'].isin(['present_less_than_ql', 'nondetect'])==False) & (df['Characteristic_Name'].isin(['air_temperature','water_temperature'])==False)
    problems = df[mask]
    if len(problems) > 0:
        print("--------------------------------------------------------------------------------")
        print(f'WARNING: {len(problems)} results from {len(problems.activity_group_id.unique())} verified activity_group_ids had `num_result` <= 0 but were not flagged nondetect or p<ql\nResolve these warnings by flagging these results\nE.g.,')
        print("")
        for x in problems.activity_group_id.unique()[:2]:
            mask = (problems['activity_group_id']==x)
            print(problems[mask][['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag']])

    # TODO: compare flags against known-correct flags so we don't export "other" entries
    FLAGS = [ # A list of known-acceptable flags
        'permanently_missing'
        ,'not_on_datasheet'
        ,'present_not_on_datasheet'
        ,'present_less_than_ql'
        ,'present_greater_than_ql'
        ,'nondetect'
        ,'value_below_mdl_actual_reported'
        ,'value_below_mdl_method_limit_reported'
        ,'value_below_rl_actual_reported'
        ,'equipment_malfunction'
    ] 
    statuses = ['verified']
    mask = (df['review_status'].isin(statuses)) & (df['data_quality_flag'].isin(FLAGS)==False) & (df['data_quality_flag'].isna()==False)
    problems = df[mask].copy()
    if len(problems) > 0:
        print("--------------------------------------------------------------------------------")
        print(f'WARNING: {len(problems)} results from {len(problems.activity_group_id.unique())} verified activity_group_ids had `data_quality_flag` of other\nResolve these warnings by updating the result flag in S123\nE.g.,')
        if len(problems) < 100:
            for x in problems.activity_group_id.unique():
                mask = (problems['activity_group_id']==x)
                print(problems[mask][['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag']])
        else:
            print(f'There are {len(problems)} warnings. Printing the first 2 site visits...')
            for x in problems.activity_group_id.unique()[:2]:
                mask = (problems['activity_group_id']==x)
                print(problems[mask][['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag']])

    # TODO: warn when C, TDS, sal are reported <2007 as YSI 100, they need to be moved to a results-were-calculated increment and blanked-out in the YSI100 increment
    # TODO: warn when a flag has a result missing or permanently missing flag but there's a result present
    # TODO: warn when a result is missing but it has no flag

    statuses = ['verified']
    mask = (df['review_status'].isin(statuses)) & (df['Result_Text'].isna()) & (df['grouping_var']=='NCRN_WQ_WQUALITY') & (df['ysi_probe'].isna()==False)

    # check ysi entries against list of known-acceptable probes
    PROBES = [
        'ysi_85'
        ,'ysi_63'
        ,'ysi_100'
        ,'ysi_63_or_85'
        ,'ysi_pro_plus'
        ,'ysi_pro_dss'
        ,'calculated_result'
        ,'Accumet'
    ]
    statuses = ['verified']
    mask = (df['review_status'].isin(statuses)) & (df['ysi_probe'].isin(PROBES)==False) & (df['grouping_var']=='NCRN_WQ_WQUALITY') & (df['ysi_probe'].isna()==False)
    problems = df[mask].copy()
    if len(problems) > 0:
        print("--------------------------------------------------------------------------------")
        print(f'WARNING: {len(problems)} results from {len(problems.activity_group_id.unique())} verified activity_group_ids had `ysi_probe` of other\nResolve these warnings by updating the ysi probe in S123\nE.g.,')
        for x in problems.activity_group_id.unique()[:2]:
            mask = (problems['activity_group_id']==x)
            print(problems[mask][['activity_group_id','record_reviewers','review_status','review_date','Characteristic_Name','num_result','data_quality_flag','ysi_probe']])

    # check flowtracker entries against list of known-acceptable probes
    PROBES = [
        'flowtracker_2'
        ,'flowtracker'
        ,'marsh_mcbirney_2000'
    ]
    statuses = ['verified']
    mask = (df['review_status'].isin(statuses)) & (df['Characteristic_Name']=='discharge_instrument') & (df['Result_Text'].isin(PROBES)==False) & (df['grouping_var']=='NCRN_WQ_WQUANTITY') & (df['sampleability']=='Actively Sampled') & (df['visit_type']=='Discrete')
    problems = df[mask].copy()
    if len(problems) > 0:
        print("--------------------------------------------------------------------------------")
        print(f'WARNING: {len(problems)} results from {len(problems.activity_group_id.unique())} verified activity_group_ids had `ysi_probe` of other\nResolve these warnings by updating the ysi probe in S123\nE.g.,')
        print("")
        for x in problems.activity_group_id.unique()[:2]:
            mask = (problems['activity_group_id']==x)
            print(problems[mask][['activity_group_id','record_reviewers','sampleability','review_status','review_date','Characteristic_Name','Result_Text']])

    return df
    