import pandas as pd
import geopandas as gpd
import sqlite3
import os
import warnings
import shapely

def connect_to_od(spath):
    '''
    Creates connection to LODES sqlite db

    :param str spath: Path to database.
    '''
    import sqlite3
    import os
    try:
        if os.path.exists(spath):
            conn = sqlite3.connect(spath)
            conn.enable_load_extension(True)
            conn.execute('SELECT load_extension("mod_spatialite")')
            crsr = conn.cursor()
            return conn,crsr
    except:
        print("No SQLite db at given path")
        return
    
def generate_query(data_type:str = 'wac',perspective:str = 'home',job_type:str='all',subset_type:str = '',state_code:str='tx',year:str='2021',geocodes=False):
    '''
    Generates query to pull from LODES database. 

    :param str data_type: Select 'wac','rac', or 'od'.
    :param str perspective: Select 'home' or 'work'. This is only relevant for o-d, ignored in wac or rac
    :param str job_type: Select 'all' or 'primary'. This is difference between JT00 and JT01
    :param str subset_type: Option to select for a specific OD-pattern for a subset of jobs, i.e. SA01 for workers under age 29
    :param str state_code: Two digit state code name, defaults to 'tx'. Useful if you have multiple states in one db. 
    :param str year: Year of data to use.
    :param str or list or DataFrame geocodes: Pass geocodes to use in query.
    '''

    import pandas as pd 

    #process geocodes
    #we need the geocodes as a string list for the query
    #check if its dataframe
    if type(geocodes) == pd.core.frame.DataFrame:
        gcs = "('" + "', '".join(geocodes["geocode"].unique()) + "')"
    #check if its list
    elif isinstance(geocodes, list):
        gcs = "('" + "', '".join(geocodes) + "')"
    #check if its string
    elif isinstance(geocodes, str):
        if geocodes[0] == '(':
            gcs = geocodes
        else:
            gcs = f"('{geocodes}')"
    else:
        print("Error: Geocodes must be a string, list, or pandas DataFrame.")
        return

    #process job_type 
    if job_type == 'all':
        jt = 'JT00'
    elif job_type == 'primary':
        jt = 'JT01'
    else:
        print(f'Warning: Building function with {job_type} as job_type')
        jt = job_type

    #process S-subset
    #process tables for query 
    if subset_type == '':
        st = 'S000_'
    else:
        print(f'Warning: Building function with {subset_type} as subset_type')
        st = f"{subset_type}_"
    
    #get geocode name
    if data_type == 'wac':
        geo_name = 'w_geocode'
    elif data_type == 'rac':
        geo_name = 'h_geocode'
    elif (data_type == 'od') and (perspective == 'home'):
        geo_name = 'h_geocode'
    elif (data_type == 'od') and (perspective == 'work'):
        geo_name = 'w_geocode'
    else:
        print("Error: Invalid data_type")

    #build a table name
    if data_type == 'od':
        table_spec = f"main_{jt}"
    elif data_type in ['wac','rac']:
        table_spec = f"{st}{jt}"
    else:
        print("Error: Could not create a coherent table name")
        return
    
    table_name = f"{state_code}_{data_type}_{table_spec}_{year}"
    
    #build an index name
    if data_type == 'od':
        index_col = f"{table_name}_od_{geo_name.replace('_','')}_index"
    elif data_type in ['wac','rac']:
        index_col = f"{table_name}_main_index"

    #build a query
    query = f"""SELECT * from {table_name} indexed by {index_col} WHERE {geo_name} IN {gcs};"""
    return query

def retype(df = None):
    '''
    Renames columns and casts the type as float for the output of LODES pull function. 
    :param pandas.core.DataFrame.DataFrame df: dataframe output of pull_data function
    '''

    import pandas as pd
    
    #1 rename columns
    remap_dict = {
        "SA01": "Age_un_29",
        "SA02": "Age_30_54",
        "SA03": "Age_55up",
        "SE03": "ov3333",
        "S000": "total",
        "SE01": "un1250",
        "SE02": "un3333",
        "SE03": "ov3333",
        "SI01": "goods",
        "SI02": "transp",
        "SI03": "other",
        "C000": "tot",
        "CA01": "Age_un_29",
        "CA02": "Age_30_54",
        "CA03": "Age_55up",
        "CE01": "Under1250",
        "CE02": "Ov1250Un3333",
        "CE03": "Over3333",
        "CNS01": "Ag_11",
        "CNS02": "OilExt_21",
        "CNS03": "Utilit_22",
        "CNS04": "Constr_23",
        "CNS05": "Mfrg_31_33",
        "CNS06": "Whlesle_42",
        "CNS07": "Retail_44_45",
        "CNS08": "TransWrh_48_49",
        "CNS09": "Info_51",
        "CNS10": "FIRE_52",
        "CNS11": "RealEst_53",
        "CNS12": "ProfSci_54",
        "CNS13": "Mgmt_55",
        "CNS14": "AdminWaste_56",
        "CNS15": "Edu_61",
        "CNS16": "Health_62",
        "CNS17": "ArtsRec_71",
        "CNS18": "AccomFood_72",
        "CNS19": "Other_81",
        "CNS20": "PubAdm_92",
        "CR01": "WhiteAl",
        "CR02": "BlackAl",
        "CR03": "AmIndAl",
        "CR04": "AsianAl",
        "CR05": "NatHawAl",
        "CR07": "TwoOrMoreAl",
        "CT01": "NotHisp",
        "CT02": "Hisp",
        "CD01": "LessThanHS",
        "CD02": "HSNoCol",
        "CD03": "AsSomeCol",
        "CD04": "BaAb",
        "CS01": "Male",
        "CS02": "Female",
        "CFA01": "BizAge0_1",
        "CFA02": "BizAge2_3",
        "CFA03": "BizAge4_5",
        "CFA04": "BizAge6_10",
        "CFA05": "BizAgeOv11",
        "CFS01": "BizSize0_19",
        "CFS02": "BizSize20_49",
        "CFS03": "BizSize50_249",
        "CFS04": "BizSize250_499",
        "CFS05": "BizSizeOv500"}
    df_new = df.rename(columns=remap_dict)

    #2 make them all numeric
    for x in df_new.columns:
        if 'geocode' not in x:
            df_new[x] = pd.to_numeric(df_new.loc[:, x], errors="coerce")
    
    df_new.fillna(0,inplace=True)

    return df_new 

def pull_data(query:str='',crsr=False,spath=False,rename=False):
    '''
    Pulls data from LODES database. 

    :param str query: Output of generate_query function.
    :param str crsr: If you've already connected and have an active cursor, you can use this. Otherwise, it will use spath.
    :param str spath: Path to the location of the LODES database.
    :param bool retype: If true, the data will get retyped using the retype function. If false, it won't. Default is false.
    '''
    import sqlite3
    import pandas as pd
    
    #generate connection
    try:
        if (spath == False) & (crsr == False):
            print("Must pass a DB path or a cursor to existing DB.")
            return
        elif (spath == False) and (crsr != False):
            crsr = crsr
        elif (type(spath) == str):
            _,crsr = connect_to_od(spath)
        else: 
            crsr = crsr
    except:
        print("Could not generate a cursor.")
        return
    
    #pull in the data
    try:  
        new_cur = crsr.execute(query)
        recs = new_cur.fetchall()
        cols = list(map(lambda x: x[0], new_cur.description))

    except:
        print("Could not get data.")
        try: 
            tn = query.split("from ")[-1].split(" indexed")[0]
            ct = crsr.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{tn}';").fetchall()[0][0]
            if ct < 1:
                print("Table does not exist in database. Check query parameters")
                return
        except:
            return

    #build the dataframe 
    try:
        df = pd.DataFrame.from_records(recs, columns=cols)
        if rename == True:
            df_out = retype(df)
        elif rename == False: 
            df_out = df.copy()
        return df_out
    except:
        print("Could not build dataframe")
        return

def transform_to_wkt(gdf = None, out_crs: int = 4326):
    """
    Prepare geodataframe to be given to query against the DataBase.
    Returns wkt of the geometry or a series of wkt geometries. Default crs is 4326.

    :param gdf: Geodataframe containing location you want to look at.
    :param int out_crs: EPSG code for output.
    """
    import warnings
    import shapely
    warnings.simplefilter(action="ignore", category=FutureWarning)
    
    gdf = gdf.to_crs(f"EPSG:{out_crs}")
    wkt = gdf.apply(lambda x: shapely.wkt.dumps(x.geometry), axis=1)
    return wkt

def id_intersections(wkt, spath=False, crsr=False, centroid=False,return_geom=False,geom_type = 'blocks',year="2020"):
    """
    Return blocks that intersect for a given polygon.
    :param str wkt: Point to query in CRS EPSG 4326 to query in WKT format
    :param str spath: Path to SQLite DB with spatial data in it, preferred.
    :param str crsr: If crsr passed, you can bypass new connection. Can also pass a crsr.
    :param bool centroid: If true, only check centroids, if false get all intersections. 
    :param bool return_geom: If true, returns geom of specified table. 
    :param str geom_type: Defaults to blocks. Can pass other things though to get zctas, etc.
    :param str year: Year to query. Defaults to 2020.
    """
    import pandas as pd
    import geopandas as gpd

    try:
        if (spath == False) & (crsr == False):
            print("Must pass a DB path or a cursor to existing DB.")
            return
        elif (spath == False) and (crsr != False):
            crsr = crsr
        elif (type(spath) == str):
            _,crsr = connect_to_od(spath)
        else: 
            crsr = crsr
    except:
        print("Could not generate a cursor.")
        return
    
    ##build up the query
    #handle if you want geom returned
    if return_geom == True:
        wkt_q = ",ST_AsText(geom) as wkt_geom"
    else:
        wkt_q = ''

    #handle if you want centroid
    if centroid == True:
        geom_q = "ST_Centroid(geom)"
    elif centroid == False:
        geom_q = "geom"

    #handle the id column you are looking for
    if geom_type == 'blocks':
        geom_type_q ='blocks'
        geocode_q = 'geocode'
    elif geom_type == 'zcta':
        geom_type_q ='zcta'
        geocode_q = 'GEOID20'
    elif geom_type == 'tracts':
        geom_type_q ='tracts'
        geocode_q = 'GEOID'
    else: 
        print(f"{geom_type} is not a valid geom_type")
        return
    
    #design query
    sq = f"""SELECT {geocode_q} as geocode{wkt_q}
            FROM {geom_type_q}_{year}_geom
            WHERE ST_Intersects({geom_q},ST_GeomFromText('{wkt}')) = 1
            AND ROWID IN (
                SELECT ROWID
                FROM SpatialIndex
                WHERE f_table_name = '{geom_type_q}_{year}_geom'
                and f_geometry_column = 'geom'
                AND search_frame = ST_GeomFromText('{wkt}'))"""
    # run spatial query
    new_cur = crsr.execute(sq)
    recs = new_cur.fetchall()
    cols = list(map(lambda x: x[0], new_cur.description))
    
    try:
        df = pd.DataFrame.from_records(recs, columns=cols)
        if 'wkt_geom' in df.columns:
            
            gdf = gpd.GeoDataFrame(df,geometry=gpd.GeoSeries.from_wkt(df['wkt_geom']),crs='EPSG:4326')
            return gdf
        return df
    except:
        print("Could not build dataframe")
        return

def pull_geometries(geocodes, spath=False, crsr=False, geom_type = 'blocks',year="2020"):
    """
    Return blocks for a list of geocodes or a dataframe.
    :param str or list or DataFrame geocodes: Pass geocodes to use in query.
    :param str spath: Path to SQLite DB with spatial data in it, preferred.
    :param str crsr: If crsr passed, you can bypass new connection. Can also pass a crsr.
    :param str geom_type: Defaults to blocks. Can pass other things though to get zctas, etc.
    :param str year: Year to query. Defaults to 2020.
    """
    import pandas as pd
    import geopandas as gpd

    try:
        if (spath == False) & (crsr == False):
            print("Must pass a DB path or a cursor to existing DB.")
            return
        elif (spath == False) and (crsr != False):
            crsr = crsr
        elif (type(spath) == str):
            _,crsr = connect_to_od(spath)
        else: 
            crsr = crsr
    except:
        print("Could not generate a cursor.")
        return
    
    if geom_type == 'blocks':
        geom_type_q ='blocks'
        geocode_q = 'geocode'
    elif geom_type == 'zcta':
        geom_type_q ='zcta'
        geocode_q = 'GEOID20'
    elif geom_type == 'tracts':
        geom_type_q ='tracts'
        geocode_q = 'GEOID'
    else: 
        print(f"{geom_type} is not a valid geom_type")
        return

    #process geocodes
    #we need the geocodes as a string list for the query
    #check if its dataframe
    if geocodes == 'all':
        gcs = ''
    if type(geocodes) == pd.core.frame.DataFrame: 
        gcs = f"WHERE {geocode_q} in" + "('" + "', '".join(geocodes["geocode"].unique()) + "')"
    #check if its list
    elif isinstance(geocodes, list):
        gcs = f"WHERE {geocode_q} in" + "('" + "', '".join(geocodes) + "')"
    #check if its string
    elif isinstance(geocodes, str):
        if geocodes[0] == '(':
            gcs = geocodes
            gcs = f"WHERE {geocode_q} in" + gcs
        else:
            gcs = f"('{geocodes}')"
            gcs = f"WHERE {geocode_q} in" + gcs
    else:
        print("Warning: Geocodes must be a string, list, or pandas DataFrame.")
        gcs = geocodes
        return

    
    #design query
    sq = f"""SELECT {geocode_q} as geocode, AsText(geom) as wkt_geom 
            FROM {geom_type_q}_{year}_geom indexed by {geom_type_q}_index {gcs}"""
    # run spatial query
    new_cur = crsr.execute(sq)
    recs = new_cur.fetchall()
    cols = list(map(lambda x: x[0], new_cur.description))
    
    try:
        df = pd.DataFrame.from_records(recs, columns=cols)
        gdf = gpd.GeoDataFrame(df,geometry=gpd.GeoSeries.from_wkt(df['wkt_geom']),crs='EPSG:4326')
        return gdf
    except:
        print("Could not build geodataframe")
        return


