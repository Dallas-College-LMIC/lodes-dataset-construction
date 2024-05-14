# LODES Dataset Construction and Analysis 
### [Dallas College LMIC](https://www.dallascollege.edu/business-industry/lmic/pages/default.aspx)
### [Link to Census LODES Conference Presentation 5/14/24](https://dcccd-my.sharepoint.com/:b:/g/personal/cmg0003_dcccd_edu/EaBivKBfW0lPhPhbRKtQixABMVPj92AGfRYFMC2JLEgBLA?e=nClNY0)
## Introduction

This library contains custom scripts to prepare a SQLite database utilizing Census LEHD LODES data. There are Python scripts to download, unzip, and save to disk files from one or multiple states' LODES data. The scripts also contain functions to facilitate the creation of a Spatialite database which enables a user to query, aggregate, and analyze LODES data using SQL queries. There are also scripts which offer basics for connecting to the SQLite database in Python, which allows for querying and pulling data into Jupyer Notebooks or other Python-based analysis frameworks. 

## Name
LODES Dataset Construction Workflow

## Description
This project has a few key functions. The first key use is to allow a user to download and unzip an entire state's LODES dataset- all OD, RAC, and WAC files. The second key use is to allow a user to build a Spatialite database with the organized LODES data. Lastly, the scripts have utility for loading in geometries into the Spatialite database.

## Usage
1. Download
2. Unzip
3. Build Spatialite
4. Load LODES into Spatialite
5. Load geometries into Spatialite.
6. Download, unzip, and load all data into Spatialite.

## Support
Contact cgilchriest@dallascollege.edu or lmic@dallascollege.edu

## Roadmap
- More complex analysis functions for college-specific applications
- Visualization functions 

## Authors and acknowledgment
Developed by Camille Gilchriest while at Dallas College LMIC, Fall 2022. Code uploaded Spring 2024. Siginificant revision and organization support from Ammar Nanjiani. 

## Project status
This project is in progress.

