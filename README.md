# Geospatial Analysis of DC Parking Violations (2021-2023)



## Project Description

This repo contains the datasets and preprocessing/transformation code for a Tableau geospatial analysis of parking 
violations in Washington, DC from 2021 to 2023.

Although I've never received a parking ticket after many, many years of driving in D.C., I was interested in exploring 
the data to see if there were any interesting patterns in how/when/where parking violations are enforced throughout the 
city. Related questions that interest me include:

* How does parking enforcement (i.e. occurrence, severity, or fines) vary by neighborhood– especially in areas of 
  historic over-policing or high rates of gentrification?
* Can we identify any patterns with respect to the parking enforcement agency (e.g. MPD, DPW, etc.), the type of 
  violation (e.g. parking in a bus zone, illegal zone parking, etc.), and ticket dispositions (if/how cases are
  resolved)?
* How does parking enforcement vary by time of day, day of week, or month of year? Volume of traffic? 
  Proximity to schools and public transportation hubs?

Once I wrangle some pre-2021, I'd also like to explore:
* How has parking enforcement varied with changes in urban economic activity due the COVID-19 pandemic?



## Data Sources

### _DC Parking Violations_

Data retrieved from [Open Data DC](https://opendata.dc.gov/datasets/parking-violations-in-the-district-of-columbia/data?geometry=-77.119%2C38.791%2C-76.909%2C38.995) under Creative Common License 4.0.

Includes 32 CSVs of parking violations from 
[January 2021](https://opendata.dc.gov/datasets/DCGIS::parking-violations-issued-in-january-2021/explore) to 
[August 2023](https://opendata.dc.gov/datasets/DCGIS::parking-violations-issued-in-august-2023/explore).
Each CSV contains 1 month of parking violations, and together they sum to approximately 3.1 million lines. Attributes 
and fields can be found at the links above.

_(Data from intermediate months can be found by using the same URL, replacing year and month values.)_


### _DC Parking Beats_
Data retrieved from 
[Open Data DC](https://opendata.dc.gov/datasets/parking-beats/data?geometry=-77.119%2C38.791%2C-76.909%2C38.995) under 
Creative Common License 4.0.

Includes 1 [Shapefile](https://opendata.dc.gov/datasets/DCGIS::dpw-parking-beats/explore) of parking beats, which are 
the geographic areas that parking enforcement officers patrol. Attributes and fields can be found at the link above.


### _DC Census Tracts_

Data retrieved from US Census Bureau under the public domain.

Includes 1 [Shapefile](https://www.census.gov/geographies/mapping-files/time-series/geo/cartographic-boundary.html) of 
2022 DC census tracts.



## Instructions for generating violation shapefile
This already exists in the repo as `data/violations_shapefile.zip`, but if you want to regenerate it, follow these steps:

1. Install [Python 3.10.0](https://www.python.org/downloads/) or later.
2. Clone this repo to your local machine and navigate into the repo directory.
3. [OPTIONAL] Create a virtual environment for this project:

MacOS/Linux:
``` commandline
python3 -m venv venv
source venv/bin/activate
```
Windows:
``` commandline
python -m venv venv
venv\Scripts\activate
```
4. Install the required packages:
``` commandline
pip install -r requirements.txt
```
5. Run the transformation/preprocessing script:
MacOS/Linux:
``` commandline
python3 transform.py
```
Windows:
``` commandline
python transform.py
```