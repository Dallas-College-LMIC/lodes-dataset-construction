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
    '''
    Create a new SQLite Database with SpatialLite enabled.
    :param str spath: Path to the location to save the SQLite db. 
    '''
    import sqlite3
    import os
    print("building sqlite db...")
    try:
        #removes existing 
        if os.path.exists(spath):
            os.remove(spath)
        #create database engine and create sqlite database
        conn = sqlite3.connect(spath)
        conn.enable_load_extension(True)

        #create spatial tables
        conn.execute('SELECT load_extension("mod_spatialite")')   
        conn.execute('SELECT InitSpatialMetaData(1);')  
        conn.close()
        print(f"success!\nsaved to: {spath}")
    except:
        print(f"could not create sqlite db at: {spath}")

def read_in_data(file_path : str = None) -> 'pandas.core.frame.DataFrame':
    """
    Read the LODES data into memory, assign it a data year, and return it
    :param str file_path: Path to the given location of the given file. 
    """
    import pandas as pd 

    #read in data 
    df = pd.read_csv(file_path,header=0,dtype="string[pyarrow]", on_bad_lines='skip',encoding = "ISO-8859-1")
    
    #assign year
    df['year'] = file_path.split("_")[-1][:4]

    return df

def create_and_insert_fast(frame:'pandas.core.frame.DataFrame',tname:str,index_col:str,index_name:str,spath:str):
    """
    Write a pandas DataFrame into a Sqlite table quickly.

    :param pandas.core.frame.DataFrame frame: DataFrame containing data you would like to upload.
    :param str tname: Name to call table in Sqlite database.
    :param str index_col: Column in DataFrame to use as an index.
    :param str index_name: Name to call index in sqlite table.
    :param str spath: Path to existing Sqlite table.
    """
    
    import sqlite3

    #connect to sqlite table
    try:
        cnx = sqlite3.connect(spath)
        cursor = cnx.cursor()
        cursor.execute(f"DROP TABLE {tname};")
        cnx.execute("PRAGMA max_page_count = 2147483646;")
        cnx.commit()
        #print("dropped old table...")
    except:
        print(f"{tname}: could not connect")
        return

    #write to database in 50k row chunks
    try:
        #print(f"writing to db...")
        n = 50000  #chunk row size
        list_df = [frame[i:i+n] for i in range(0,frame.shape[0],n)]
        for chunk_frame in list_df:
            chunk_frame.to_sql(name=tname, con=cnx,if_exists="append", index=False)
    except:
        print(f"{tname}: could not write")
        return

    #create index
    try:
        if (type(index_col) == str) and (type(index_name) == str):
            crsr = cnx.cursor()
            #drop existing index and remake
            crsr.execute(f"DROP INDEX IF EXISTS {index_name}")
            crsr.execute(f"CREATE INDEX {index_name} ON {tname} ({index_col})")
    except:
        print(f"{tname}: could not make index")
        return

    try:
        #commit everything and close connection
        cnx.commit()
        cnx.close()
        return
    except:
        print(f"{tname}: error closing")
        return
    

def write_spatial_table_into_db(gdf:'geopandas.geodataframe.GeoDataFrame', tname:str = '',geom_col:str = 'geometry',
    index_col:str = '',index_name:str = '',keep_cols:list = [],spath:str = ''):
    '''
    Write spatial dataframe into sqlite db. Designed to use geodataframe, with any given index column. 
    Default creates a spatial index on the geometry column. Uses shapely to make geometry wkt.

    :param geopandas.GeoDataFrame gdf: GeoDataFrame you want in the Spatialite table.
    :param str tname: Name you want for the table in SQLite DB. Passed to create_insert_fast.
    :param str geom_col: Name of the column to use for geometry.
    :param str index_col: Name of the column to use as a non-spatial index column.
    :param str index_name: Name of non-spatial index column.
    :param list keep_cols: Optional parameter of additional columns to retain in the SQLite DB.
    :param str spath: Path to SQLite database.
    '''

    import sqlite3
    import geopandas as gpd 
    import shapely
    gpd.options.use_pygeos = True
    
    print(f'processing {tname}')

    #slice dataframe into relevant parts
    try:
        gdf = gdf[[index_col,geom_col] + keep_cols].copy()
    except:
        print(f"{tname}: error slicing")
        return
    
    #process for upload
    try:
        #convert to EPSG 4326
        gdf = gdf.to_crs("EPSG:4326")

        #drop na geometries
        gdf = gdf.loc[~gdf[geom_col].isna()]
    
        #convert to wkb representation
        gdf['wkb_geometry'] = gdf.apply(lambda x: shapely.wkb.dumps(x[geom_col],output_dimension=2), axis=1)
        
        #drop geometry
        gdf = gdf.drop(columns=[geom_col])

    except:
        print(f"{tname}: error preparing for upload")
        return
        
    
    # write data into sqlite database
    try:
        print(f'writing {tname} into database.')
        create_and_insert_fast(frame=gdf, 
            tname=tname, 
            index_col=index_col,
            index_name=index_name,
            spath=spath)
    except:
        print(f"{tname}: error preparing for upload")
        return

    # make sqlite spatial
    try:
        #load extensions
        conn = sqlite3.connect(spath)
        conn.enable_load_extension(True)
        conn.execute('SELECT load_extension("mod_spatialite")')

        #start cursor
        crsr = conn.cursor()

        #add multipolygon geometry column to original table
        crsr.execute(f"SELECT AddGeometryColumn('{tname}', 'geom', 4326, 'MULTIPOLYGON', 2);")
        
        # update the yet empty geom column by parsing the well-known-binary objects from 
        # the geometry column into Spatialite geometry objects
        
        crsr.execute(f"UPDATE {tname} SET geom=ST_Multi(GeomFromWKB(wkb_geometry, 4326));")
        # drop the other geometry column which are not needed anymore 
        # unfortunately, there is no DROP COLUMN support in sqlite3, 
        # so a heavy workaround is needed to clean up via a temporary table.
        # get a list of columns to use as the main columns you want, adding in the new geom column
        # automatically will exclude wkb_geometry column
        columns = str(tuple(gdf.columns.tolist() + ['geom'])).replace("(","").replace(")","").replace("'","")
        
        #create backup table with relevant data
        crsr.execute(f"CREATE TABLE {tname}_backup({columns});")
        crsr.execute(f"INSERT INTO {tname}_backup SELECT * from {tname};")
        
        #drop original table
        crsr.execute(f"DROP TABLE {tname};")

        #create new table with columns and name to match desired table
        crsr.execute(f"CREATE TABLE {tname}({columns});")
    
        #put everything back into original table
        crsr.execute(f"INSERT INTO {tname} SELECT * FROM {tname}_backup;")
        crsr.execute(f"DROP TABLE {tname}_backup;")

        conn.commit()
        crsr.close()
        conn.close()
    except:
        print(f"{tname}: error making geometry")
        return

    #make spatial index
    try:
        #reconnect 
        conn = sqlite3.connect(spath)
        conn.enable_load_extension(True)
        conn.execute('SELECT load_extension("mod_spatialite")')

        #start cursor
        crsr = conn.cursor()
         #create index
        crsr.execute(f"DROP INDEX IF EXISTS {tname}_index")
        crsr.execute(f"CREATE INDEX {index_name} ON {tname}({index_col})")
        
        #create spatial index on geometry
        crsr.execute(f"SELECT CreateSpatialIndex('{tname}', 'geom');")
        
        #commit and close
        conn.commit()
        crsr.close()
        conn.close()
    except:
        print(f"{tname}: error making spatial index")
    print(f"processed {tname}")

def load_blocks(geom_w:str = None):
    '''
    Prepares the blocks data to be loaded into the Spatialite

    :param str geom_w: Path to the location of GeoDataBase containing the blocks. Sourced from NHGIS.
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
    '''
    Reads and then loads all the LODES tabular data into Spatialite db. 

    :param str folder_path: Path to the location of unzipped lodes data; output of the unzip_all() functions.
    :param str spath: Path to the location of Spatialite database.
    '''

    import time 
    import sqlite3

    try:
        #get the file paths into 3 bunches
        racs,wacs,ods = get_file_paths(folder_path=fr"{folder_path}\**\*.*")
    except:
        print("could not find file paths")
        return

    #load in racs
    try:
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
    except:
        print("rac upload unsuccessful")
            
    #load in wacs
    try:
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

    except:
        print("wac upload unsuccessful")

    #load in od 
    try:
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
    except:
        print("od upload unsuccessful")  

    print("done loading all in")

def load_geometries_into_db(spath : str = None):
    '''
    Reads and then loads into the database a series of geometry files for reference. 
    Largely custom; check paths and files. Original geometry data is from NHGIS. 

    :param str spath: Path to the location of Spatialite database.
    '''
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

    gdf:'geopandas.geodataframe.GeoDataFrame', tname:str = '',geom_col:str = 'geometry',
    index_col:str = '',index_name:str = '',keep_cols:list = [],spath:str = ''


    write_spatial_table_into_db(gdf = b_gdf, 
        tname = "blocks_2020_geom", 
        geom_col = 'geometry',
        index_col="geocode",
        index_name="blocks_index", 
        keep_cols=['GISJOIN','STATEFP20','COUNTYFP20','TRACTCE20'],
        spath=spath)

    start = time.strftime("%H:%M:%S")
    print(f"write tract start time: {start}")  
    write_spatial_table_into_db(gdf = t_gdf, 
        tname = "tracts_2020_geom", 
        geom_col = 'geometry',
        index_col="GEOID",
        index_name="tracts_index", 
        keep_cols=['STATEFP','COUNTYFP','TRACTCE'],
        spath=spath)
        
    start = time.strftime("%H:%M:%S")
    print(f"write zcta start time: {start}")  
    write_spatial_table_into_db(gdf = z_gdf, 
        tname = "zcta_2020_geom", 
        geom_col = 'geometry',
        index_col ="GEOID20",
        index_name ="zcta_index", 
        keep_cols=['ZCTA5CE20','MTFCC20','GISJOIN'],
        spath=spath)

    end = time.strftime("%H:%M:%S")
    print(f"full geom load in end time: {end}")   
    print("done loading up geometries")

