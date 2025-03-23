# Project Structure

```
surf-spot-aggregator/
│
├── README.md                   # Project overview and documentation
├── requirements.txt            # Python dependencies
├── main.py                     # Main orchestration script
├── .gitignore                  # Files to be ignored by git
│
├── scraper/                    # Web scraping modules
│   ├── __init__.py             # Make directory a proper Python package
│   ├── break_list_scraper.py   # Scrapes the list of surf breaks
│   └── break_detail_scraper.py # Scrapes detailed information for each break
│
├── data_processing/            # Data processing and integration modules
│   ├── __init__.py             # Make directory a proper Python package
│   ├── country_standardizer.py # Standardizes country names
│   └── data_merger.py          # Merges data from different sources
│
├── utils/                      # Utility functions
│   ├── __init__.py             # Make directory a proper Python package
│   └── cleaning_utils.py       # Functions for cleaning and processing data
│
└── data/                       # Directory for storing data (git-ignored)
    ├── surf_breaks_list.csv            # List of surf breaks
    ├── surf_breaks_complete.csv        # Detailed surf break information
    ├── surf_breaks_complete_standardized.csv   # With standardized country names
    ├── additional_source_complete_standardized.csv # Second source data
    ├── merged_surf_breaks.csv          # Merged dataset
    ├── source1_unmatched.csv           # Unmatched entries from source 1
    └── source2_unmatched.csv           # Unmatched entries from source 2
```

## Directory Purposes

- **scraper/**: Contains modules for extracting data from surf websites
- **data_processing/**: Contains modules for cleaning, standardizing, and merging datasets
- **utils/**: Contains utility functions shared across modules
- **data/**: Storage for raw and processed data files (ignored by git)

## Key Files

- **main.py**: Orchestrates the entire data collection and processing pipeline
- **requirements.txt**: Lists all Python dependencies for the project
- **README.md**: Documentation on project setup, usage, and purpose
- **break_list_scraper.py**: Scrapes the initial list of surf breaks
- **break_detail_scraper.py**: Scrapes detailed information for each surf break
- **country_standardizer.py**: Standardizes country names across datasets
- **data_merger.py**: Merges data from multiple sources
- **cleaning_utils.py**: Provides utility functions for data cleaning
