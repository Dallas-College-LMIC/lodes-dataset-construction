
'''
Script for building OD database files into a sqlite db
'''

def get_file_paths(folder_path: str = None):
    """
    Get filepaths to a list of files in a common place separated by years.
    :param str folder_path: Path to the given location of the given file. 
    """
    import glob
    import os 
    import re 
    files = glob.glob(folder_path,recursive=True)
    racs = []
    wacs = []
    ods = []
    for q in files:
        if q.endswith(".csv"):
            if 'rac' in q:
                racs.append(q)
            elif 'wac' in q:
                wacs.append(q)
            elif 'od' in q:
                ods.append(q)
    return racs,wacs,ods

def build_db(spath : str = None):
    import sqlite3
    import os
    '''
    Create a new SQLite Database with SpatialLite enabled.

   :param str spath: Path to the location to save the SQLite db. 
   '''
    print("Building sqlite db...")
    try:
        #removes existing 
        if os.path.exists(spath):
            os.remove(spath)
        # create database engine and create sqlite database
        conn = sqlite3.connect(spath)
        conn.enable_load_extension(True)

        #create spatial tables
        conn.execute('SELECT load_extension("mod_spatialite")')   
        conn.execute('SELECT InitSpatialMetaData(1);')  
        conn.close()
        print(f"Success!\nSaved to: {spath}")
    except:
        print(f"Could not create SQLite db at: {spath}")

def read_in_data(file_path : str = None):
    """
   Read in DCthe LODES data, give it an a year, and return it.

   :param str file_path: Path to the given location of the given file. 
   """
    import pandas as pd 

    #read in data 
    #print(f"Reading in {year} using csv method")
    df = pd.read_csv(file_path,header=0,dtype="string[pyarrow]", on_bad_lines='skip',encoding = "ISO-8859-1")
    df['year'] = file_path.split("_")[-1][:4]
    #df = df.fillna("None")
    #print("Done")
    return df

def create_and_insert_fast(frame,tname,index_col,index_name,spath):
    """
    Write a file into a table in SQL quickly.

    :param str file_path: Path to the given location of the given file. 
    :param str or int year: Year as an int or str
    """
    import sqlite3
    #print(f"connecting to db: {spath}")
    cnx = sqlite3.connect(spath)
    try:
        cursor = cnx.cursor()
        cursor.execute(f"DROP TABLE {tname};")
        cnx.execute("PRAGMA max_page_count = 2147483646;")
        cnx.commit()
        #print("dropped old table...")
    except:
        None
    #cnx.execute("PRAGMA max_page_count = 2147483646;")
    #cnx.commit()
    try:
        #print(f"writing to db...")
        n = 50000  #chunk row size
        list_df = [frame[i:i+n] for i in range(0,frame.shape[0],n)]
        for chunk_frame in list_df:
            chunk_frame.to_sql(name=tname, con=cnx,if_exists="append", index=False)
    except:
        print(f"error- could not write {tname}")
    #create index
    try:
        if (type(index_col) == str) and (type(index_name) == str):
            #print("creating index...")
            crsr = cnx.cursor()
            crsr.execute(f"DROP INDEX IF EXISTS {index_name}")
            crsr.execute(f"CREATE INDEX {index_name} ON {tname} ({index_col})")
    except:
        print("could not make index")
    try:
        cnx.commit()
        cnx.close()
        #print("done")
        return
    except:
        print("error closing")
        return
    #print("done writing")

def write_spatial_table_into_db(gdf, table_name,spath,index_name,index_col,keep_cols=[]):
    '''
    Write spatial dataframe into sqlite db. Designed to use geodataframe, with any given index column.
    
    :param geopandas.GeoDataFrame gdf: GeoDataFrame you want in the SQLIte table.
    :param str table_name: Name you want for the table in SQLite DB. Passed to create_insert_fast.
    :param str spath: Path to SQLite table.
    :param str index_col: Name of the column to use as a non-spatial index column.
    :param str index_name: Name of non-spatial index column.
    :param list keep_cols: Optional parameter of additional columns to retain in the SQLite DB.
    '''
    import sqlite3
    import geopandas as gpd 
    import shapely

    print(f'Processing {table_name}')

    #slice dataframe into relevant parts
    gdf = gdf[[index_col,'geometry'] + keep_cols].copy()

    #convert to EPSG 4326
    gpd.options.use_pygeos = True
    gdf = gdf.to_crs("EPSG:4326")

    #convert to wkb representation
    print("Converting geometry...")
    #drop na geometries
    gdf = gdf.loc[~gdf['geometry'].isna()]
    gdf['wkb_geometry'] = gdf.apply(lambda x: shapely.wkb.dumps(x.geometry,output_dimension=2), axis=1)
    gdf = gdf.drop(columns='geometry')
    
    # write data into sqlite database
    print(f'Writing {table_name} into database.')
    create_and_insert_fast(frame=gdf, 
        tname=table_name, 
        index_col=index_col,
        index_name=index_name,
        spath=spath)

    #make spatial
    try:
        print("Spatial operations...")
        #load extensions
        conn = sqlite3.connect(spath)
        conn.enable_load_extension(True)
        conn.execute('SELECT load_extension("mod_spatialite")')

        #start cursor
        crsr = conn.cursor()

        #add multipolygon geometry column to original table
        crsr.execute(f"SELECT AddGeometryColumn('{table_name}', 'geom', 4326, 'MULTIPOLYGON', 2);")
        
        # update the yet empty geom column by parsing the well-known-binary objects from the geometry column into Spatialite geometry objects
        print("Setting geometries...")
        crsr.execute(f"UPDATE {table_name} SET geom=ST_Multi(GeomFromWKB(wkb_geometry, 4326));")
        # drop the other geometry column which are not needed anymore. 
        # unfortunately, there is no DROP COLUMN support in sqlite3, 
        # so a heavy workaround is needed to clean up via a temporary table.
        print("Cleaning up old table...")
        #get a list of columns to use as the main columns you want, adding in the new geom column
        #automatically will exclude wkb_geometry column
        columns = str(tuple(gdf.columns.tolist() + ['geom'])).replace("(","").replace(")","").replace("'","")
        
        #create backup table with relevant data
        crsr.execute(f"CREATE TABLE {table_name}_backup({columns});")
        crsr.execute(f"INSERT INTO {table_name}_backup SELECT * from {table_name};")
        
        #drop original table
        crsr.execute(f"DROP TABLE {table_name};")

        #create new table with columns and name to match desired table
        crsr.execute(f"CREATE TABLE {table_name}({columns});")

        #put everything back into original table
        crsr.execute(f"INSERT INTO {table_name} SELECT * FROM {table_name}_backup;")
        crsr.execute(f"DROP TABLE {table_name}_backup;")
        
        #create index on geoid
        #due to the write/rewrite function you do need this to keep index
        print("Create regular index..")
        crsr.execute(f"DROP INDEX IF EXISTS {table_name}_index")
        crsr.execute(f"CREATE INDEX {index_name} ON {table_name}({index_col})")
        
        #create spatial index on geometry
        print("Create spatial index..")
        crsr.execute(f"SELECT CreateSpatialIndex('{table_name}', 'geom');")
        
        #commit and close
        conn.commit()
        crsr.close()
        conn.close()
        print(f"Done processing {table_name}\n")
    except:
        print(f"Error on spatial operations for {table_name}.\n")

def load_blocks(geom_w:str = None):
    '''
    Prepares the blocks data  to be loaded into the Spatialite

   :param str geom_w: Path to the location of GeoDataBase containing the blocks. Layers must be as specified. Sourced from NHGIS.
   '''
    
    import geopandas as gpd
    print(f"loading blocks geometries...")
    gdf = gpd.read_file(geom_w,layer="Blocks_Texas_NHGIS_2020")
    try:
        gdf['geocode'] = gdf['GEOID20']
    except:
        print("No column called geocode")
    gdf.drop(columns=['GEOID20'],inplace=True)
    print("Done")
    return gdf

def load_lodes_into_db(folder_path:str = None,spath:str = None):

    import time 
    import sqlite3
    #get the file paths into 3 bunches
    racs,wacs,ods= get_file_paths(folder_path=fr"{folder_path}\**\*.*")

    #load in racs
    start = time.strftime("%H:%M:%S")
    print(f"rac start time: {start}")                
    counter = len(racs)
    for i,q in enumerate(racs):
        if (i % 50 == 0) or (i+1 == counter):
            print(f"{((i+1)/counter):.1%} complete...")
        try:
            #read in
            table_name = q.split("\\")[-1].replace(".csv","")
            dfm = read_in_data(file_path = q)
            #upload
            create_and_insert_fast(frame=dfm, 
                tname=table_name,
                index_col="h_geocode",
                index_name=f"{table_name}_main_index",
                spath=spath) 
        except:
            print(f"error on {q}")

    end = time.strftime("%H:%M:%S")
    print(f"rac end time: {end}")     

    #load in wacs
    start = time.strftime("%H:%M:%S")
    print(f"wac start time: {start}")                
    counter = len(wacs)
    for i,q in enumerate(wacs):
        if (i % 50 == 0) or (i+1 == counter):
            print(f"{((i+1)/counter):.1%} complete...")
        try:
            #read in
            table_name = q.split("\\")[-1].replace(".csv","")
            dfm = read_in_data(file_path = q)
            #upload
            create_and_insert_fast(frame=dfm, 
                tname=table_name,
                index_col="w_geocode",
                index_name=f"{table_name}_main_index",
                spath=spath) 
        except:
            print(f"error on {q}")

    end = time.strftime("%H:%M:%S")
    print(f"wac end time: {end}")     

    #load in od 
    start = time.strftime("%H:%M:%S")
    print(f"od start time: {start}")                
    counter = len(ods)
    for i,q in enumerate(ods):
        if (i % 50 == 0) or (i+1 == counter):
            print(f"{((i+1)/counter):.1%} complete...")
        try:
            #read in
            table_name = q.split("\\")[-1].replace(".csv","")
            dfm = read_in_data(file_path = q)
            #upload
            create_and_insert_fast(frame=dfm, 
                tname=table_name,
                index_col="h_geocode",
                index_name=f"{table_name}_od_hgeocode_index",
                spath=spath) 
            cnx = sqlite3.connect(spath)
            crsr = cnx.cursor()
            crsr.execute(f"DROP INDEX IF EXISTS {table_name}_od_wgeocode_index")
            crsr.execute(f"CREATE INDEX {table_name}_od_wgeocode_index ON {table_name} (w_geocode)")
        except:
            print(f"error on {q}")

    end = time.strftime("%H:%M:%S")
    print(f"od end time: {end}")     
    print("done loading all in")

def load_up_geometries(spath : str = None):
    
    import geopandas as gpd
    import time 
    
    start = time.strftime("%H:%M:%S")
    print(f"load blocks start time: {start}")  
    b_gdf = load_blocks(r"C:\Users\cmg0003\Desktop\BackgroundLayers.gdb")
    b_gdf = b_gdf.to_crs("EPSG:4326")

    start = time.strftime("%H:%M:%S")
    print(f"load tracts start time: {start}")  
    t_gdf = gpd.read_file(r"C:\Users\cmg0003\Desktop\BackgroundLayers.gdb",layer="Tracts_State_NHGIS_Projected_2020")
    t_gdf = t_gdf.to_crs("EPSG:4326")

    start = time.strftime("%H:%M:%S")
    print(f"load zcta start time: {start}")  
    z_gdf = gpd.read_file(r"C:\Users\cmg0003\Desktop\BackgroundLayers.gdb",layer="ZCTA_State_NHGIS_Projected_2020")
    z_gdf = z_gdf.to_crs("EPSG:4326")

    start = time.strftime("%H:%M:%S")
    print(f"write blocks start time: {start}")  
    write_spatial_table_into_db(b_gdf, 
        "blocks_2020_geom", 
        spath=spath, 
        index_name="blocks_index", 
        index_col="geocode",
        keep_cols=['GISJOIN','STATEFP20','COUNTYFP20','TRACTCE20'])

    start = time.strftime("%H:%M:%S")
    print(f"write tract start time: {start}")  
    write_spatial_table_into_db(t_gdf, 
        "tracts_2020_geom", 
        spath=spath, 
        index_name="tracts_index", 
        index_col="GEOID",
        keep_cols=['STATEFP','COUNTYFP','TRACTCE'])

    start = time.strftime("%H:%M:%S")
    print(f"write zcta start time: {start}")  
    write_spatial_table_into_db(z_gdf, 
        "zcta_2020_geom", 
        spath=spath, 
        index_name="tracts_index", 
        index_col="GEOID20",
        keep_cols=['ZCTA5CE20','MTFCC20','GISJOIN'])
    
    end = time.strftime("%H:%M:%S")
    print(f"full geom load in end time: {end}")   
    print("done loading up geometries")

