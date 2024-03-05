
"""
this script demonstrates how you can build a lodes spatialdb from your scratch
the example is for texas but it would work for any state
note: the load_up_geometries function at the end would likely require the most work for a state other than texas
"""

from download_and_unzip import *
from build_database import *
import os 

#define paths
wkd = r"C:\Users\cmg0003\Desktop\TX_Lodes_Download"
spath = os.path.join(wkd,"lodes_tx.db")

# --- processing
#get all the potential files
fps = get_all_possible_files(save=True,
                             savepath=wkd,
                             savename="state_dict")


#this downloads everything in that state's lodes
state_fold = download_state_lodes_file(save_loc=wkd,
                          st='tx',
                          links_dict=fps)

#this unzips everything 
unzip_state_lodes_file(state_fold= state_fold)

#state_fold = r"C:\Users\cmg0003\Desktop\TX_Lodes_Download\tx"

# loads downloaded data into spatialite 
build_db(spath=spath) #be careful - this build function overwrites existing data
load_lodes_into_db(folder_path = state_fold,spath = spath)
load_geometries_into_db(spath=spath) #note - this is basically a custom function for texas geometries - will need work for other states
