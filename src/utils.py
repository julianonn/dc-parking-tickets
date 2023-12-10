import pandas as pd
import numpy as np
import datetime as dt
from rapidfuzz import fuzz, process, utils
import pytz
import re
import requests
import io

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'

# regex for fuzzy matching
numbered_street = r"([0-9]+)(st|nd|rd|th)"
letter_street = r"[a-z]+"
street_types = r"st|street|ave|avenue|pl|place|dr|drive|ct|court|blvd|boulevard|ln|lane|rd|road|pkwy|parkway|cir|circle|ter|terrace|plz|plaza|way|alley|row|roadway|walk|walkway|square|sq"
street = f"(?:^|\s)({numbered_street}|{letter_street}) ({street_types})(?:\s|$)"

BLOCK = r"(?:^|\s)([0-9]+)(\s(block|blk))?(?:\s|$)"
STREET = re.compile(street)
QUADRANT = r" (nw|ne|sw|se)(?:\s|$)"


null_vals = {None, pd.NA, np.nan, '', ' ', '  '}
def clear_trivial_values(s: set):
    return s - null_vals



def clean_string(s: str):
    if isinstance(s, str):
        return re.sub(r'[^A-Za-z0-9 ]+', '', s.strip().lower())
    else:
        return s


def fetch_file_urls():
    """
        Returns a list of urls for the raw csv files on github
    """
    gh_url = "https://raw.githubusercontent.com/julianonn/dc-parking-tickets/master/data/raw_monthly_parking_violations_csvs/"

    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
              "November", "December"]
    years = ["2021", "2022", "2023"]

    url_list = []
    for year in years:
        for month in months:
            fname = f"Parking_Violations_Issued_in_{month}_{year}.csv"
            # we have no data for Sept-Dec 2023
            if not (year == "2023" and month in {"September", "October", "November", "December"}):
                url_list.append(gh_url + fname)

    return url_list


def url_to_df(url: str):
    """
        Takes a url and returns a dataframe
    """
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    return df


def int_to_time(x):
    """
        formats the parking data "issued time" column (which is a really stupid integer) into a datetime.time object 
        function called by df.apply in clean.py
    """
    str_time = str(x).zfill(4)

    # Convert the string to a datetime object
    try:
        time_obj = dt.datetime.strptime(str_time, "%H%M")
        est = pytz.timezone('US/Eastern')
        time_obj = est.localize(time_obj)
        return time_obj.time()
    except:
        return pd.NaT


def link_lat_long(location_map: dict, index: int, loc:str, lat_long: str):
    """ 
        function called by df.apply in clean.py
    """
    if loc in null_vals:
        return ''
    if loc in location_map:
        if pd.isna(lat_long):
            return location_map[loc][index]
    return loc


def validate_fuzzy_match(unknown: str, match: str):
    """
    one  day i'll add documentation
    """
    unknown_tup = (
        re.search(BLOCK, unknown),
        re.search(STREET, unknown),
        re.search(QUADRANT, unknown)
    )
    known_tup = (
        re.search(BLOCK, match),
        re.search(STREET, match),
        re.search(QUADRANT, match)
    )

    if any([x is None for x in unknown_tup]) or any([x is None for x in known_tup]):
        return False

    # Block is okay if the first two digits are the same or if it differs by one trailing 0
    unknown_block = unknown_tup[0].group(1).strip()
    known_block = known_tup[0].group(1).strip()
    if unknown_block != known_block:
        d1 = str(unknown_tup[0].group(1).strip()[0:2])
        d2 = str(known_tup[0].group(1).strip()[0:2])

        if d1 != d2:
            if d1 == d2 + "0" or d2 == d1 + "0":  # differs by one trailing 0
                pass  # okay!
            else:
                return False
        else:
            pass  # okay!

    # rule out non-matching streets
    unknown_street = unknown_tup[1].group(1).strip()
    known_street = known_tup[1].group(1).strip()
    if unknown_street != known_street:
        return False

    # rule out non-matching quadrants
    unknown_quad = unknown_tup[2].group(1).strip()
    known_quad = known_tup[2].group(1).strip()
    if unknown_quad != known_quad:
        return False

    return True  # finally, if all cases pass, return true


# end validate_block_street_match()


def get_best_fuzzy_match(possible_matches: set, query: str):
    """ applied vectorized to the dataframe """
    tup = process.extractOne(
        query, possible_matches, scorer=fuzz.WRatio
    )  # (match, score, index?)

    return tup[0]  # best match
