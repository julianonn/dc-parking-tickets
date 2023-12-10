import pandas as pd
import numpy as np
import datetime as dt
from rapidfuzz import fuzz, process
import pytz
import re
import requests
import io
import urllib3

requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL:@SECLEVEL=1'


# ============================================================================
# =========================== REGEX FOR VALIDATION ===========================
# ============================================================================

numbered_street = r"([0-9]+)(st|nd|rd|th)"
letter_street = r"[a-z]+"
street_types = "st|street|ave|avenue|pl|place|dr|drive|ct|court|blvd|boulevard" \
               "|ln|lane|rd|road|pkwy|parkway|cir|circle|ter|terrace|plz|plaza" \
               "|way|alley|row|roadway|walk|walkway|square|sq"
street = f"(?:^|\\s)({numbered_street}|{letter_street}) ({street_types})(?:\\s|$)"

STREET = re.compile(street)
BLOCK = r"(?:^|\s)([0-9]+)(\s(block|blk))?(?:\s|$)"
QUADRANT = r" (nw|ne|sw|se)(?:\s|$)"

# ============================================================================
# ============================== STRING CLEANING =============================
# ============================================================================

null_vals = {None, pd.NA, np.nan, '', ' ', '  '}


def clear_trivial_values(s: set):
    """
    Removes trivial values from a set (e.g. None, np.nan, '', ' ', '  ')

    Parameters:
            s: set of values
        Returns:
            s - null_vals: set of values with trivial values removed
    """
    return s - null_vals


def clean_string(s: str):
    """
        Cleans a string by removing non-alphanumeric characters and converting to lowercase
        Parameters:
            s: string to be cleaned
        Returns:
            s: cleaned string
    """

    if isinstance(s, str):
        return re.sub(r'[^A-Za-z0-9 ]+', '', s.strip().lower())
    else:
        return s


# ============================================================================
# ============================== FETCH/URL METHODS ===========================
# ============================================================================


def fetch_file_urls():
    """
        Fetches the urls for the raw parking ticket csv files from my public repo
        Returns:
            url_list: list of urls for the raw csv files on github
    """
    gh_url = "https://raw.githubusercontent.com/julianonn/dc-parking-tickets" \
             "/master/data/raw_monthly_parking_violations_csvs/"

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
        Reads in a csv file from url and returns a dataframe

        Parameters:
            url: url for the raw csv file
        Returns:
            df: dataframe containing the raw data from the csv file
    """
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    return df


# ============================================================================
# ========================= DATE-TIME TRANSFORM METHODS ======================
# ============================================================================
def int_to_time(x):
    """
        formats the parking data "issued time" column (which is a really stupid integer) into a datetime.time object 
        function called by df.apply in transform.py

        Parameters:
            x: integer representing the time in 24-hour format (e.g. 1300)
        Returns:
            time_obj.time(): datetime.time object representing the time OR pd.NaT if the input is invalid
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


def date_to_str(elem):
    """
        converts a date object to a string in the format YYYY-MM-DD, for use in df.apply

        Parameters:
            elem: datetime.date object
        Returns:
            elem.strftime("%Y-%m-%d"): string in the format YYYY-MM-DD
    """
    if isinstance(elem, (pd.Timestamp, dt.date, dt.datetime, dt.time)):
        return elem.strftime("%Y-%m-%d")
    return str(elem)


def time_to_str(elem):
    """
        converts a time object to a string in the format HH:MM:SS, for use in df.apply

        Parameters:
            elem: datetime.time
        Returns:
            elem.strftime("%H:%M:%S"): string in the format HH:MM:SS
    """
    if isinstance(elem, (pd.Timestamp, dt.date, dt.datetime, dt.time)):
        return elem.strftime("%H:%M:%S")
    return str(elem)


# ============================================================================
# ====================== RECORD LINKAGE HELPER METHODS =======================
# ============================================================================
def link_lat_long(location_map: dict, index: int, loc: str, lat_long: str):
    """ 
        function called by df.apply in transform.py
    """
    if loc in null_vals:
        return lat_long
    if loc in location_map:
        if pd.isna(lat_long):
            return location_map[loc][index]
    return lat_long


# ============================================================================
# ======================== FUZZY MATCH HELPER METHODS ========================
# ============================================================================
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
        query, possible_matches, scorer=fuzz.ratio, processor=None
    )  # (match, score, index?)

    return tup[0]  # best match
