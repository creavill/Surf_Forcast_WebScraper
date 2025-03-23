#!/usr/bin/env python3
"""
Country Standardizer Module

This module standardizes country names across different datasets
to enable accurate merging of data from multiple sources.
"""

import os
import pandas as pd
import pycountry


# Define country name mapping for non-standard country names
COUNTRY_MAPPING = {
    'USA': 'United States',
    'UAE': 'United Arab Emirates',
    'UK': 'United Kingdom',
    'British Virgin': 'British Virgin Islands',
    'Virgin Islands': 'US Virgin Islands',
    'Spain (Europe)': 'Spain',
    'Turks   Caicos': 'Turks and Caicos Islands',
    'St Lucia': 'Saint Lucia',
    'St Kitts': 'Saint Kitts and Nevis',
    'Ivory Coast': "Côte d'Ivoire",
    'St Barthelemy': "Saint Barthélemy",
    'Christmas': 'Christmas Island',
    'Tobago': 'Trinidad and Tobago',
    'Solomon': 'Solomon Islands',
    'Brunei': 'Brunei Darussalam',
    'Northern Mariana Islands': 'Mariana Islands',
    'Congo': 'Republic of the Congo',
    'Cook': 'Cook Islands',
    'Faroe': 'Faroe Islands',
    'Samoa American': 'American Samoa',
    'Samoa Western': 'Samoa',
    'Cayman': 'Cayman Islands',
    'Hong Kong': 'China',
    'Spain (Africa)': 'Canary Islands',
    # Add more mappings as needed
}


def standardize_country(country):
    """
    Standardize a country name to its official name.
    
    Args:
        country (str): Country name to standardize
        
    Returns:
        str: Standardized country name
    """
    # Clean input
    country = str(country).replace('_', ' ').strip()
    
    # Check if the country is in the custom mapping
    if country in COUNTRY_MAPPING:
        return COUNTRY_MAPPING[country]
    
    # Try to find the country in pycountry
    try:
        # Search by name
        country_obj = pycountry.countries.get(name=country)
        if country_obj:
            return country_obj.name
        
        # Search by common name (alternative)
        for c in pycountry.countries:
            if hasattr(c, 'common_name') and c.common_name == country:
                return c.name
    except:
        # If unable to standardize, return the original name
        pass
    
    return country


def standardize_countries_in_file(input_file, output_file, country_column='country'):
    """
    Standardize country names in a CSV file.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        country_column (str): Name of the column containing country names
    """
    print(f"Standardizing country names in {input_file}...")
    
    # Load the CSV file
    df = pd.read_csv(input_file)
    
    # Check if the country column exists
    if country_column not in df.columns:
        print(f"Country column '{country_column}' not found in the input file!")
        return
    
    # Get the unique countries before standardization
    countries_before = sorted(df[country_column].unique())
    print(f"Found {len(countries_before)} unique countries before standardization.")
    
    # Apply the standardize_country function to the country column
    df[country_column] = df[country_column].apply(standardize_country)
    
    # Get the unique countries after standardization
    countries_after = sorted(df[country_column].unique())
    print(f"Found {len(countries_after)} unique countries after standardization.")
    
    # Save the standardized DataFrame
    df.to_csv(output_file, index=False)
    print(f"Standardized data saved to {output_file}")


def main():
    """Main function to run the country standardizer"""
    # Create data directory if it doesn't exist
    data_dir = "../data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Define input and output files
    source1_input = os.path.join(data_dir, "surf_breaks_complete.csv")
    source1_output = os.path.join(data_dir, "surf_breaks_complete_standardized.csv")
    
    source2_input = os.path.join(data_dir, "additional_source_complete.csv")
    source2_output = os.path.join(data_dir, "additional_source_complete_standardized.csv")
    
    # Check if input files exist
    if not os.path.exists(source1_input):
        print(f"Input file {source1_input} not found!")
        print("Please run break_detail_scraper.py first.")
    else:
        standardize_countries_in_file(source1_input, source1_output)
    
    if not os.path.exists(source2_input):
        print(f"Input file {source2_input} not found!")
        print("This is expected if you only have data from one source.")
    else:
        standardize_countries_in_file(source2_input, source2_output)


if __name__ == "__main__":
    main()
