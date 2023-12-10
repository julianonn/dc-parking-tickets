"""
Author : Julia Nonnenkamp


Data Wrangling for DC Parking Tickets Project

"""
import os

import pandas as pd
import geopandas as gpd
import numpy as np
import utils
import requests
import pyogrio
import shutil
import warnings

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'
warnings.filterwarnings("ignore")


# ============================================================================
# =========================== DATA READ/PREPROCESS ===========================
# ============================================================================
def _read_raw_data():
    """
        Reads in all the raw csv files from github into a dataframe with minimal preprocessing
        Returns:
            df: dataframe containing all the raw data from data/raw_monthly_parking_violations_csvs/...
    """
    # get the list of urls
    url_list = utils.fetch_file_urls()

    print("Begin file read....")
    # read in the data
    df = pd.DataFrame()

    for url in url_list:
        print("\t\t", url)
        temp = utils.url_to_df(url)

        temp['ISSUE_DATE'] = pd.to_datetime(temp['ISSUE_DATE'], errors="coerce").dt.date
        temp['ISSUE_TIME'] = temp['ISSUE_TIME'].apply(utils.int_to_time)

        df = pd.concat([df, temp])

    return df.replace('', np.nan)


# end _read_raw_data()


# ============================================================================
# ============================== SIMPLE MATCHING =============================
# ============================================================================
def _simple_fill(df: pd.DataFrame):
    """
        Fills in missing coordinates by linking exact matches of location strings to coordinates
        where those coordinates do not appear in the dataset

        Parameters:
            df: main dataframe of raw data
        Returns:
            df: main dataframe with filled in coordinates
    """
    print("\tSimple fill....")

    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]
    null_location_set = set(nulls['LOCATION'].unique())
    # print("NULLS:", nulls)

    # clear trivial values from location set
    print("\t\tClearing trivial values....", end='')
    utils.clear_trivial_values(null_location_set)
    print("\t\tDone!")

    # link records
    print("\t\tLinking null (LATITUDE, LONGITUDE) tuples to existing locations....", end='')
    link = df[df['LOCATION'].isin(null_location_set)]
    link = link[(pd.isna(link['LATITUDE'])) | (pd.isna(link['LONGITUDE']))]
    link = link[['LOCATION', 'LATITUDE', 'LONGITUDE']].drop_duplicates()
    print("\t\tDone!")

    # update the main dataframe
    location_map = link.to_dict(orient='records')
    location_map = {entry['LOCATION']: (entry['LATITUDE'], entry['LONGITUDE']) for entry in location_map}

    return _update_main_data_frame(location_map, df)


# end simple_fill()

# ============================================================================
# ============================== FUZZY MATCHING ==============================
# ============================================================================
def _fuzzy_fill(df: pd.DataFrame):
    """
        Fills in missing coordinates by linking fuzzy matches of location strings to coordinates
        where those coordinates do not appear in the dataset.

        Parameters:
            df: main dataframe of raw data
        Returns:
            df: main dataframe with filled in coordinates
    """
    print("\tFuzzy fill....")

    # --------------------- Isolating 'unfilled' coordinates ------------------- #
    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]

    nulls = nulls[['LOCATION', 'LATITUDE', 'LONGITUDE']]  # dataframe where we have lat and long
    knowns = df[(~pd.isna(df['LATITUDE'])) & (~pd.isna(df['LONGITUDE']))]
    knowns = knowns[['LOCATION', 'LATITUDE', 'LONGITUDE']]  # dataframe where we have lat and long

    nulls = nulls.drop_duplicates(subset=['LOCATION'])
    nulls = nulls.dropna(subset=['LOCATION'])
    knowns = knowns.drop_duplicates(subset=['LOCATION'])
    knowns = knowns.dropna(subset=['LOCATION'])

    # ------------------------------ Preprocessing ----------------------------- #
    print("\t\tReformatting for fuzzy matching....", end='')
    # get rid of special chars and extra whitespace and make lowercase
    nulls['formatted_location'] = nulls['LOCATION'].apply(utils.clean_string)
    knowns['formatted_location'] = knowns['LOCATION'].apply(utils.clean_string)
    print("\t\tDone!")

    # ------------------------------ Fuzzy Matching ----------------------------- #
    known_set = set(knowns['formatted_location'].unique())

    # vectorize for efficiency
    print("\t\tComputing best fuzzy matches (vectorized, although it'll still take a while)....", end='')
    vec = np.vectorize(utils.get_best_fuzzy_match, excluded=['possible_matches'])

    nulls['best_fuzzy_match'] = vec(possible_matches=known_set, query=nulls['formatted_location'])

    print("\t\tDone!")

    # ------------------------------ Match Validation ---------------------------- #
    print("\t\tValidating fuzzy matches (vectorized)....", end='')
    nulls['valid?'] = np.vectorize(utils.validate_fuzzy_match)(
        unknown=nulls['formatted_location'],
        match=nulls['best_fuzzy_match']
    )
    print("\t\tDone!")

    """
        FOR REFERENCE: 
        matches.columns = ['LOCATION', 'formatted_location', 'best_fuzzy_match', 'valid?']
        knowns.columns = ['LOCATION', 'LATITUDE', 'LONGITUDE', 'formatted_location']
        matches.best_fuzzy_match EQUALS knowns.formatted_location
    """

    matches = nulls[nulls['valid?']]
    known_dict = {row['formatted_location']: (row['LATITUDE'], row['LONGITUDE']) for _, row in knowns.iterrows()}
    matches['LATITUDE'] = matches['best_fuzzy_match'].apply(lambda x: known_dict[x][0])
    matches['LONGITUDE'] = matches['best_fuzzy_match'].apply(lambda x: known_dict[x][1])

    # ------------------------------ Update Main DF ------------------------------ #
    location_map = matches.to_dict(orient='records')
    location_map = {entry['LOCATION']: (entry['LATITUDE'], entry['LONGITUDE']) for entry in location_map}

    return _update_main_data_frame(location_map, df)


# end fuzzy_fill()


# ============================================================================
# ================================ UPDATE DF =================================
# ============================================================================
def _update_main_data_frame(location_map: dict, df: pd.DataFrame):
    """
        Helper function for fuzzy_fill() and simple_fill()

        Parameters:
            location_map: dictionary mapping location strings to (LATITUDE, LONGITUDE) tuples
            df: main dataframe
    """
    print("\t\tUpdating main dataframe (vectorized)....", end='')

    vec = np.vectorize(utils.link_lat_long, excluded=['location_map', 'index'])
    df['LATITUDE'] = vec(location_map=location_map, index=0, loc=df['LOCATION'], lat_long=df['LATITUDE'])
    df['LONGITUDE'] = vec(location_map=location_map, index=1, loc=df['LOCATION'], lat_long=df['LONGITUDE'])
    df[['LATITUDE', 'LONGITUDE']] = df[['LATITUDE', 'LONGITUDE']].replace('', np.nan)  # replace empty strings with NaNs

    print("\t\tDone!")
    return df


# end _update_main_data_frame()


# ============================================================================
# =========================== GEOSPATIAL TRANSFORM ===========================
# ============================================================================
def coordinates_to_geospatial(df: pd.DataFrame):
    """
        Converts a dataframe with LATITUDE, LONGITUDE to a geospatial dataframe
        with a field 'geometry' containing a POINT object

        Parameters:
            df: dataframe containing coordinates
        Returns:
            gdf: geospatial dataframe
    """
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE),
        crs="EPSG:4326"
    )
    return gdf


# end coordinates_to_geospatial()


def shapefile_preprocessing(gdf: gpd.GeoDataFrame):
    """
        Preprocesses the geodataframe to convert datetime objects to strings since
        shapefile read does not support datetime.

        Parameters:
            gdf: geodataframe containing the shapefile data
        Returns:
            gdf: geodataframe with datetime objects converted to strings
    """
    gdf['ISSUE_DATE'] = np.vectorize(
        lambda x: utils.date_to_str(x) if not pd.isna(x) else ''
    )(gdf['ISSUE_DATE'])

    gdf['ISSUE_TIME'] = np.vectorize(
        lambda x: utils.time_to_str(x) if not pd.isna(x) else ''
    )(gdf['ISSUE_TIME'])

    gdf['ISSUE_DATE'] = gdf['ISSUE_DATE'].astype(str)
    gdf['ISSUE_TIME'] = gdf['ISSUE_TIME'].astype(str)

    #gdf['MULTI_OWNER_NUMBER'] = gdf['MULTI_OWNER_NUMBER'].astype(str)

    keep_columns = ['OBJECTID',
                    'ISSUE_DATE',
                    'ISSUE_TIME',
                    'ISSUING_AGENCY_CODE',
                    'VIOLATION_CODE',
                    'PLATE_STATE',
                    'VEHICLE_TYPE',
                    'DISPOSITION_CODE',
                    'DISPOSITION_DATE',
                    'FINE_AMOUNT',
                    'TOTAL_PAID',
                    'PENALTY_1',
                    'PENALTY_2',
                    'PENALTY_3',
                    'PENALTY_4',
                    'PENALTY_5',
                    'LATITUDE',
                    'LONGITUDE']
    gdf = gdf[keep_columns]
    return gdf


# end shapefile_preprocessing()

def _write_zip(gdf: gpd.GeoDataFrame):
    """
        Writes the geodataframe to a zip file containing the shapefile data
    """
    d = os.path.dirname(os.path.abspath(__file__)) + "/data/violations_shapefile"
    zippath = d + ".zip"

    # remove old zip file and shapefile directory
    if os.path.exists(zippath):
        os.remove(zippath)
    if os.path.exists(d):
        shutil.rmtree(d)
    if not os.path.exists(d):
        os.mkdir(d)

    # write shapefile
    print("\t\tCreating sidecar files....", end='')
    shppath = d + "/violations.shp"
    # gdf.to_file(fpath, engine='pyogrio', driver='ESRI Shapefile'
    pyogrio.write_dataframe(gdf, shppath, encoding="utf-8", driver='ESRI Shapefile')
    print("\t\tDone!")

    # zip shapefile and sidecar files
    print("\t\tZipping....", end='')
    shutil.make_archive(d, 'zip', d)
    print("\t\tDone!")

    # delete shapefile directory
    shutil.rmtree(d)

# enf _write_zip()

# ============================================================================
# ================================ MAIN ======================================
# ============================================================================

def transform():
    df = _read_raw_data()
    df.to_csv("test-full.csv", index=False)
    # df = pd.read_csv("test-full.csv")

    print("Filling null coordinates....")
    df = _simple_fill(df)
    df = _fuzzy_fill(df)
    print("....Done!")

    print("Converting to shapefile for geospatial data....")
    gdf = coordinates_to_geospatial(df)
    gdf = shapefile_preprocessing(gdf)
    print("....Done!")

    print("Writing to shapefile....")
    _write_zip(gdf)
    print("....Done!")

    return df


# end transform()


if __name__ == "__main__":
    transform()
