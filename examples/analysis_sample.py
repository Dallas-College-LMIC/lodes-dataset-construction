
#analysis sample
from analysis import *

spath = r"D:\LODES Full DBs\TX_Lodes_Download\lodes_tx_slim.db"

con,cur = connect_to_od(spath=spath)

## 1 get rac data for an arbitrary geocode
q1 = generate_query(data_type='rac',perspective='home',job_type='primary',year='2019',geocodes='480019501001010')
rac_df = pull_data(query=q1,crsr=cur,rename=True)

## 2 get od commuteshed data for an arbitrary geocode
q = generate_query(data_type='od',perspective='home',job_type='all',year='2019',geocodes='480019501001010')
od_df = pull_data(query=q,crsr=cur,rename=True)
retype(od_df)


## 3 get a buffer around a city and then query it
#get a random example from houston
import geopandas as gpd
example = gpd.read_file(gpd.datasets.get_path('naturalearth_cities')).query("name.str.contains('Houston')")
example = example.to_crs("EPSG:2278")
example.geometry = example.buffer(5 * 5280)

#get the wkt
wkts = transform_to_wkt(gdf=example)

#get the blocks as a list that intersect it 
isect = id_intersections(wkt=wkts.iloc[0], spath=spath)['geocode'].tolist()

#run the query
qb = generate_query(data_type='wac',job_type='all',year='2020',geocodes = isect)
dfb = pull_data(qb,spath=spath,rename=True)



## 4 quickly generate a plot of the destination for workers from within a 2 mile buffer of houston
#get a random example from houston
import geopandas as gpd
example = gpd.read_file(gpd.datasets.get_path('naturalearth_cities'))
htown = example.query("name.str.contains('Houston')")
htown = htown.to_crs("EPSG:2278")
#five miles from downtown houston
htown.geometry = htown.buffer(5 * 5280)

#get the wkt
wkts = transform_to_wkt(gdf=htown)

#get the blocks as a list that intersect it 
isect_gdf = id_intersections(wkt=wkts.iloc[0], spath=spath,return_geom=True)
isect = isect_gdf['geocode'].tolist()

#run the query
qb = generate_query(data_type='od',perspective='work',job_type='primary',year='2021',geocodes = isect)
dfb = pull_data(qb,spath=spath,rename=True)
totals = dfb.groupby('h_geocode').agg({'total':'sum'}).reset_index()


#pull geometries within 50 miles 
htown50 = example.query("name.str.contains('Houston')")
htown50 = htown50.to_crs("EPSG:2278")
#five miles from downtown houston
htown50.geometry = htown50.buffer(5 * 5280)
wkts50 = transform_to_wkt(gdf=htown50)
b50mi = id_intersections(wkt=wkts50.iloc[0], spath=spath,return_geom=True)

#add in the data
m = b50mi.merge(totals,left_on='geocode',right_on='h_geocode',how='left').fillna(0)

m.plot(column='total',legend=True,figsize=(10,10))

#could easily dissolve into tracts
m['tract_id'] = m['geocode'].str[:11]
m2 = m[['tract_id','geometry','total']].dissolve(by='tract_id',aggfunc='sum')
m2.plot(column='total',legend=True,figsize=(10,10))
