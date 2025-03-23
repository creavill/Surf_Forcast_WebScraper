# Surf Spot Data Aggregator

## Project Overview
This project scrapes and aggregates surf break information from multiple sources to create a comprehensive database of global surf spots. The system extracts detailed information including location data, wave characteristics, seasonal information, and reliability ratings.

## Disclaimer
**This project is for educational purposes only.** The code demonstrates web scraping techniques, data manipulation, and data integration processes. Please respect website terms of service and robots.txt restrictions when using web scraping technologies.

## Features
- Web scraping of surf spot data from multiple sources
- Data normalization and cleaning
- Country name standardization 
- Matching and merging of datasets
- Comprehensive surf break information including:
  - Region and country information
  - Wave type and quality ratings
  - Swell and wind direction details
  - Seasonal information (best months/seasons)
  - Reliability metrics

## Project Structure
- `scraper/` - Contains the web scraping modules
  - `break_list_scraper.py` - Scrapes the list of surf breaks
  - `break_detail_scraper.py` - Scrapes detailed information for each break
- `data_processing/` - Contains the data processing and integration modules
  - `country_standardizer.py` - Standardizes country names
  - `data_merger.py` - Merges data from different sources
- `utils/` - Utility functions
  - `cleaning_utils.py` - Functions for cleaning and processing data

## Requirements
- Python 3.7+
- Required packages:
  - requests
  - beautifulsoup4
  - pandas
  - tqdm
  - pycountry

## Installation
1. Clone this repository
2. Install required packages:
```
pip install -r requirements.txt
```

## Usage
1. Run the break list scraper to collect basic information:
```
python scraper/break_list_scraper.py
```

2. Run the break detail scraper to collect detailed information:
```
python scraper/break_detail_scraper.py
```

3. Standardize country names:
```
python data_processing/country_standardizer.py
```

4. Merge data from different sources:
```
python data_processing/data_merger.py
```

## Future Improvements
- Implement automated data update scheduling
- Add visualization components for surf spot analytics
- Create a searchable database with filtering capabilities
- Develop a frontend interface for exploring the data
