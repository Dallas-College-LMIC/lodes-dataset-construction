import pandas as pd
import geopandas as gpd
import numpy as np
import sqlite3
import os
import warnings
import shapely

def connect_to_od(spath:str) -> tuple[sqlite3.Connection,sqlite3.Cursor]:
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
    
def generate_query(data_type:str = 'wac',perspective:str = 'home',job_type:str='all',subset_type:str = '',state_code:str='tx',year:[str,int,float]='2021',geocodes:[str,list,pd.core.frame.DataFrame]=False) -> str:
    '''
    Generates query to pull data from LODES database. 

    :param str data_type: Select 'wac','rac', or 'od'.
    :param str perspective: Select 'home' or 'work'. This is only relevant for o-d, ignored in wac or rac
    :param str job_type: Select 'all' or 'primary'. This is difference between JT00 and JT01
    :param str subset_type: Option to select for a specific OD-pattern for a subset of jobs, i.e. SA01 for workers under age 29
    :param str state_code: Two digit state code name, defaults to 'tx'. Useful if you have multiple states in one db. 
    :param str,int,float year: Year of data to use.
    :param str,list,pd.DataFrame geocodes: Pass geocodes to use in query.
    '''

    #part 1 - process inputs 
    #process year
    try:
        year = str(year)[:4]
    except:
        print(f"Error with year '{year}'")
        return 
    
    #process geocodes (blocks) 

     #1 - check if its dataframe
    if type(geocodes) == pd.core.frame.DataFrame:
        try: 
            gcs = "('" + "', '".join(geocodes["geocode"].unique()) + "')"
        except:
            print("Error - must name the geocode column 'geocode'")
            return

     #2 - check if its list
    elif isinstance(geocodes, list):
        gcs = "('" + "', '".join(geocodes) + "')"
    
     #3 - check if its string
    elif isinstance(geocodes, str):
        if geocodes[0] == '(':
            gcs = geocodes
        else:
            gcs = f"('{geocodes}')"
     #4 - spit it out if its nothing    
    else:
        print("Error: Geocodes must be a string, list, or pandas DataFrame.")
        return

    #reject perspective if not 'home' or 'work'
    if data_type == 'od':
        if perspective not in ['home','work']:
            print(fr"Error: '{perspective}' passed as perspective.\nMust pass 'home' or 'work'")
            return

    #process job_type - corresponds to JT in LODES documentation 
    if job_type == 'all':
        jt = 'JT00'
    elif job_type == 'primary':
        jt = 'JT01'
    else:
        print(f'Warning: Building function with {job_type} as job_type')
        jt = job_type

    #process S-subset - segment of workforce in LODES documentation
        #defaults to all
    if subset_type == '':
        st = 'S000_'
    else:
        print(f'Warning: Building function with {subset_type} as subset_type')
        st = f"{subset_type}_"
    
    #based on data type get the column that will match in the table
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
        #doesn't have room to handle aux files- this is a future error/fixable thing
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

def retype(df:pd.core.frame.DataFrame = None) -> pd.core.frame.DataFrame:
    '''
    Renames columns and casts the type as float for the output of LODES pull function. 
    :param pd.DataFrame df: dataframe output of pull_data function
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

def pull_data(query:str='',crsr:sqlite3.Cursor=False,spath:str=False,rename:bool=False):
    '''
    Pulls data from LODES database based on the output of generate query function. 

    :param str query: Output of generate_query function.
    :param sqlite3.Cursor crsr: If you've already connected and have an active cursor, you can use this. Otherwise, it will use spath.
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

def transform_to_wkt(gdf:gpd.geodataframe.GeoDataFrame = None, out_crs: int = 4326) -> gpd.geodataframe.GeoDataFrame:
    """
    Utility function to return WellKnownText of the geometry column of a GeoDataFrame. Default crs is 4326.

    :param gdf: Geodataframe containing location you want to look at.
    :param int out_crs: EPSG code for output.
    """
    
    gdf = gdf.to_crs(f"EPSG:{out_crs}")
    wkt = gdf.apply(lambda x: shapely.wkt.dumps(x.geometry), axis=1)
    return wkt

def id_intersections(wkt:str,crsr:sqlite3.Cursor=False, spath:str=False,centroid:bool=False,return_geom:bool=False,geom_type:bool = 'blocks',year:[int,str,float]="2020"):
    """
    Return geometries that intersect for a given polygon.
    :param str wkt: Polygon to query against geometries in Spatialite db in CRS EPSG 4326.
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

    #handle if you want centroid, i.e. those geometries that intersect only with the center
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
    Utility to return geometries for a list of geocodes or a dataframe.
    :param str or list or DataFrame geocodes: Pass geocodes to use in query.
    :param str spath: Path to SQLite DB with spatial data in it, preferred.
    :param str crsr: If crsr passed, you can bypass new connection. Can also pass a crsr.
    :param str geom_type: Defaults to blocks. Can pass other things though to get zctas, etc.
    :param str year: Year to query. Defaults to 2020.
    """
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
    elif type(geocodes) == pd.core.frame.DataFrame: 
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


def rank_mapper(
    df,
    title="Rank of Home Cities of Hutchins Workers, 2002-2019",
    cityname_column="work_city",
    savepath=r"P:\Data\RESEARCH\FY23\LMIC\111 - Forney Commute Patterns - CG\Outputs\od_workers_rank.png",):
    """
    Return a rank map chart.
    :param DataFrame df: df formatted correctly.
    :param str title: Title you'd like to use.
    :param str cityname_column: Column with destination or origin city in it
    :param str savepath: Filename and path to output
    """
    # get top cities over time

    top_city_orig = df.reset_index().sort_values(
        by=["year", "total"], ascending=[True, False]
    )

    yr_top_city = pd.DataFrame()
    for x in top_city_orig["year"].unique():
        y = top_city_orig.query("year == @x").head(15)
        y["rank"] = range(1, 16)
        yr_top_city = pd.concat([yr_top_city, y])

    # create matrix

    yr_top_matrix = (
        yr_top_city[["year", cityname_column, "rank"]]
        .pivot(index=[cityname_column], columns=["year"])
        .fillna(0)
    )
    yr_tot_matrix = (
        yr_top_city[["year", cityname_column, "total"]]
        .pivot(index=[cityname_column], columns=["year"])
        .fillna(0)
    )

    list_of_labels_display = []
    list_of_labels_iter = []
    for y, z in zip(yr_top_matrix.columns, yr_tot_matrix.columns):
        labels = yr_top_matrix.sort_values(by=y, ascending=True)[y].drop(
            yr_top_matrix[yr_top_matrix[y] == 0].index
        )
        labels_num = yr_tot_matrix.sort_values(by=z, ascending=False)[z].drop(
            yr_tot_matrix[yr_tot_matrix[z] == 0].index
        )
        list_of_labels_iter.append(labels.index)
        list_of_labels_display.append(
            list([f"{q}\n({r:,.0f})" for q, r in zip(labels.index, labels_num)])
        )
    label_array = np.transpose(np.array(list_of_labels_iter))
    label_disp_array = np.transpose(np.array(list_of_labels_display))

    # rank change
    geoarray = label_array
    rowcount = geoarray.shape[0]
    colcount = geoarray.shape[1]

    # Create a number of blank lists
    changelist = [[] for _ in range(rowcount)]

    for i in range(colcount):
        if i == 0:
            # Rank change for 1st year is 0, as there is no previous year
            for j in range(rowcount):
                changelist[j].append(0)
        else:
            col = geoarray[:, i]  # Get all values in this col
            prevcol = geoarray[:, i - 1]  # Get all values in previous col
            for v in col:
                array_pos = np.where(col == v)  # returns array
                current_pos = int(array_pos[0])  # get first array value
                array_pos2 = np.where(prevcol == v)  # returns array
                if (
                    len(array_pos2[0]) == 0
                ):  # if array is empty, because place was not in previous year
                    previous_pos = current_pos + 1
                else:
                    previous_pos = int(array_pos2[0])  # get first array value
                if current_pos == previous_pos:
                    changelist[current_pos].append(0)
                    # No change in rank
                elif current_pos > previous_pos:  # Larger value = smaller rank
                    changelist[current_pos].append(-1)
                elif current_pos < previous_pos:  # Larger value = smaller rank
                    changelist[current_pos].append(1)
                    # Rank has decreased
                else:
                    pass

    rankchange = np.array(changelist)

    list_of_totals = []
    for y in yr_tot_matrix.columns:
        totals = yr_tot_matrix.sort_values(by=y, ascending=False)[y].drop(
            yr_tot_matrix[yr_tot_matrix[y] == 0].index
        )
        list_of_totals.append(list(totals.values))
    totals_array = np.transpose(np.array(list_of_totals))

    # make plot
    import matplotlib.pyplot as plt
    from matplotlib import colors

    alabels = label_disp_array
    yrs = list(top_city_orig["year"].unique())
    xlabels = yrs
    ylabels = [
        "1st",
        "2nd",
        "3rd",
        "4th",
        "5th",
        "6th",
        "7th",
        "8th",
        "9th",
        "10th",
        "11th",
        "12th",
        "13th",
        "14th",
        "15th",
    ]

    mycolors = colors.ListedColormap(["#de425b", "#f7f7f7", "#67a9cf"])
    fig, ax = plt.subplots(figsize=(22, 22))
    im = ax.imshow(rankchange, cmap=mycolors)

    # Show all ticks...
    ax.set_xticks(np.arange(len(xlabels)))
    ax.set_yticks(np.arange(len(ylabels)))
    # ... and label them with the respective list entries
    ax.set_xticklabels(xlabels)
    ax.set_yticklabels(ylabels)

    # Create white grid.
    ax.set_xticks(np.arange(totals_array.shape[1] + 1) - 0.5, minor=True)
    ax.set_yticks(np.arange(totals_array.shape[0] + 1) - 0.5, minor=True)
    ax.grid(which="minor", color="gray", alpha=0.5, linestyle="-", linewidth=2)
    ax.grid(which="major", visible=False)
    ax.set_xlabel(
        "Source: Dallas College Labor Market Intelligence Center",
        loc="right",
        fontsize=13,
    )

    cbar = ax.figure.colorbar(im, ax=ax, ticks=[1, 0, -1], shrink=0.5)
    cbar.ax.set_yticklabels(
        ["Higher Rank YOY", "No Change", "Lower Rank YOY"], fontsize=18
    )

    # Loop over data dimensions and create text annotations.
    for i in range(len(ylabels)):
        for j in range(len(xlabels)):
            lab = alabels[i, j].split(" ")
            if len(lab) > 1:
                label = lab[0] + "\n " + " ".join(lab[1:])
            else:
                label = lab[0]
            text = ax.text(
                j, i, label, ha="center", va="center", color="black", fontsize=10.25
            )
    ax.set_title(title, fontsize=20)
    plt.xticks(fontsize=18)
    plt.yticks(fontsize=18)
    fig.tight_layout()
    plt.savefig(savepath, dpi=600, facecolor="None")
    plt.show()
