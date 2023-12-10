"""
Author : Julia Nonnenkamp


Data Wrangling for DC Parking Tickets Project

"""
import pandas as pd
import numpy as np
import utils
import requests

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


def read_raw_data():
    """
        Reads in all the raw csv files from github and returns a dataframe with minimal preprocessing
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


# end read_raw_data()


def simple_fill(df: pd.DataFrame):
    """
        Fills in missing coordinates by linking exact matches of location strings to coordinates
        where those coordinates do not appear in the dataset
    """
    print("\tSimple fill....")

    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]
    null_location_set = set(nulls['LOCATION'].unique())
    print("NULLS:", nulls)

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


def fuzzy_fill(df: pd.DataFrame):
    """
        Fills in missing coordinates by linking fuzzy matches of location strings to coordinates
        where those coordinates do not appear in the dataset
    """
    print("\tFuzzy fill....")
    # partition the dataframe into knowns and unknowns
    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]

    nulls = nulls[['LOCATION', 'LATITUDE', 'LONGITUDE']]  # dataframe where we have lat and long
    knowns = df[(~pd.isna(df['LATITUDE'])) & (~pd.isna(df['LONGITUDE']))]
    knowns = knowns[['LOCATION', 'LATITUDE', 'LONGITUDE']]  # dataframe where we have lat and long

    nulls = nulls.drop_duplicates(subset=['LOCATION'])
    nulls = nulls.dropna(subset=['LOCATION'])
    knowns = knowns.drop_duplicates(subset=['LOCATION'])
    knowns = knowns.dropna(subset=['LOCATION'])

    print("\t\tReformatting for fuzzy matching....", end='')
    # get rid of special chars and extra whitespace and make lowercase
    nulls['formatted_location'] = nulls['LOCATION'].apply(utils.clean_string)
    knowns['formatted_location'] = knowns['LOCATION'].apply(utils.clean_string)
    print("\t\tDone!")

    # compute best fuzzy match
    known_set = set(knowns['formatted_location'].unique())

    # vectorize for efficiency
    print("\t\tComputing best fuzzy matches (vectorized, although it'll still take a while)....", end='')
    vec = np.vectorize(utils.get_best_fuzzy_match, excluded=['possible_matches'])

    print("NULLS:\n", nulls)
    print("KNOWN:\n", knowns)

    nulls['best_fuzzy_match'] = vec(possible_matches=known_set, query=nulls['formatted_location'])
    print("\t\tDone!")

    # validate fuzzy matches
    print("\t\tValidating fuzzy matches (vectorized)....", end='')
    nulls['valid?'] = np.vectorize(utils.validate_fuzzy_match)(
        unknown=nulls['formatted_location'],
        match=nulls['best_fuzzy_match']
    )
    print("\t\tDone!")

    # FOR REFERENCE: 
    # matches.columns = ['LOCATION', 'formatted_location', 'best_fuzzy_match', 'valid?']
    # knowns.columns = ['LOCATION', 'LATITUDE', 'LONGITUDE', 'formatted_location']
    # matches.best_fuzzy_match EQUALS knowns.formatted_location

    matches = nulls[nulls['valid?'] == True]
    known_dict = {row['formatted_location']: (row['LATITUDE'], row['LONGITUDE']) for _, row in knowns.iterrows()}
    matches['LATITUDE'] = matches['best_fuzzy_match'].apply(lambda x: known_dict[x][0])
    matches['LONGITUDE'] = matches['best_fuzzy_match'].apply(lambda x: known_dict[x][1])

    # update the main dataframe
    location_map = matches.to_dict(orient='records')
    location_map = {entry['LOCATION']: (entry['LATITUDE'], entry['LONGITUDE']) for entry in location_map}

    return _update_main_data_frame(location_map, df)


def _update_main_data_frame(location_map: dict, df: pd.DataFrame):
    """
        Helper function for fuzzy_fill() and simple_fill()
    """
    print("\t\tUpdating main dataframe (vectorized)....", end='')
    vec = np.vectorize(utils.link_lat_long, excluded=['location_map', 'index'])
    df['LATITUDE'] = vec(location_map=location_map, index=0, loc=df['LOCATION'], lat_long=df['LATITUDE'])
    df['LONGITUDE'] = vec(location_map=location_map, index=1, loc=df['LOCATION'], lat_long=df['LONGITUDE'])
    df[['LATITUDE', 'LONGITUDE']] = df[['LATITUDE', 'LONGITUDE']].replace('', np.nan)  # replace empty strings with NaNs
    print("\t\tDone!")
    return df


if __name__ == "__main__":
    df = read_raw_data()
    df.to_csv("test-full.csv", index=False)
    #df = pd.read_csv("test-full.csv", dtype=str)

    print("Filling null coordinates....")
    df = simple_fill(df)
    #nulls = df[(df['LATITUDE'].isin(utils.null_vals)) | (df['LONGITUDE'].isin(utils.null_vals))]
    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]
    print("NULLS 2:", nulls)
    df = fuzzy_fill(df)
    nulls = df[(pd.isna(df['LATITUDE'])) | (pd.isna(df['LONGITUDE']))]
    print("NULLS 3:", nulls)
    print(df)

    print("Done!")
