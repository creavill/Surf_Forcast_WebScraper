#!/usr/bin/env python3
"""
Break Detail Scraper Module

This module scrapes detailed information for each surf break
using the links collected by the break_list_scraper.
"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm


def scrape_break_details(breaks_df, base_domain="PLACEHOLDER_DOMAIN"):
    """
    Scrape detailed information for each surf break.
    
    Args:
        breaks_df (pd.DataFrame): DataFrame containing break names, links, and countries
        base_domain (str): Base domain for surf forecast website
        
    Returns:
        pd.DataFrame: DataFrame with detailed break information
    """
    # Create a copy of the input DataFrame
    df = breaks_df.copy()
    
    # Create new columns to store the scraped data
    df['region'] = ''
    df['type'] = ''
    df['rating'] = ''
    df['reliability'] = ''
    df['swell_direction'] = ''
    df['wind_direction'] = ''
    df['best_month'] = ''
    df['best_season'] = ''
    df['summary'] = ''
    df['time_of_year'] = ''

    # Iterate through each break
    print("Scraping detailed break information...")
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Processing breaks"):
        url = f"https://{base_domain}{row['link']}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract region information
            df.at[index, 'region'] = extract_region(soup)
            
            # Extract country information (from the page, not the list)
            df.at[index, 'country'] = extract_country(soup)
            
            # Extract break information from the details table
            extract_break_info(soup, df, index)
            
            # Extract directions, seasonal info, and summary
            extract_additional_info(soup, df, index)
            
        except requests.exceptions.RequestException as e:
            print(f"Error scraping break {row['name']}: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error processing break {row['name']}: {e}")
            continue
            
    return df


def extract_region(soup):
    """Extract region information from the page"""
    try:
        region_select = soup.find('select', id='region_id')
        region_option = region_select.find('option', selected=True)
        return region_option.text if region_option else ''
    except:
        return ''


def extract_country(soup):
    """Extract country information from the page"""
    try:
        country_select = soup.find('select', id='country_id')
        country_option = country_select.find('option', selected=True)
        return country_option.text if country_option else ''
    except:
        return ''


def extract_break_info(soup, df, index):
    """Extract break information from the details table"""
    # Find the table with the guide-header__information class
    table = soup.find('table', class_='guide-header__information')
    if not table:
        return
        
    try:
        # Find the type
        type_img = table.find('img', class_='guide-header__type-icon guide-header__type-icon--break')
        if type_img:
            type_text = type_img.find_next_sibling(text=True)
            df.at[index, 'type'] = type_text.strip() if type_text else ''
    except:
        pass

    try:
        # Find the rating
        rating_img = table.find('img', class_='guide-header__type-icon guide-header__type-icon--stars')
        if rating_img:
            rating_span = rating_img.find_next_sibling('span')
            df.at[index, 'rating'] = rating_span.text if rating_span else ''
    except:
        pass

    try:
        # Find the reliability
        tds = table.find_all('td')
        if len(tds) > 2:
            df.at[index, 'reliability'] = tds[2].text.strip()
    except:
        pass


def extract_additional_info(soup, df, index):
    """Extract directions, seasonal info, and summary"""
    try:
        # Find the swell direction and wind direction
        p_tag = soup.find('div', class_='guide-header__best-surf').find('p')
        if p_tag:
            swell_spans = p_tag.find_all('span', class_='guide-header__dir')
            if len(swell_spans) > 0:
                df.at[index, 'swell_direction'] = swell_spans[0].text
            if len(swell_spans) > 1:
                df.at[index, 'wind_direction'] = swell_spans[1].text
    except:
        pass

    try:
        # Find the best month and best season
        best_month_div = soup.find('div', class_='guide-page__best-month')
        if best_month_div:
            df.at[index, 'best_month'] = best_month_div.text.split('Best')[0]
            season_span = best_month_div.find('span')
            if season_span and ':' in season_span.text:
                df.at[index, 'best_season'] = season_span.text.split(': ')[1]
    except:
        pass

    try:
        # Find the summary
        summary_div = soup.find('div', class_='guide-header__summary__text')
        if summary_div:
            df.at[index, 'summary'] = summary_div.text.strip()
    except:
        pass

    try:
        # Find the time of year
        time_of_year_div = soup.find('div', class_='guide-page__text')
        if time_of_year_div:
            df.at[index, 'time_of_year'] = time_of_year_div.text.strip()
    except:
        pass


def save_data(df, output_dir="../data", filename="surf_breaks_complete.csv"):
    """
    Save the scraped data to a CSV file.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        output_dir (str): Directory to save the file to
        filename (str): Name of the output file
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save DataFrame to CSV
    output_path = os.path.join(output_dir, filename)
    df.to_csv(output_path, index=False)
    print(f"Complete data saved to {output_path}")


if __name__ == "__main__":
    input_file = "../data/surf_breaks_list.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file {input_file} not found!")
        print("Please run break_list_scraper.py first.")
        exit(1)
    
    # Load the breaks list
    breaks_df = pd.read_csv(input_file)
    
    # Scrape detailed information
    detailed_df = scrape_break_details(breaks_df)
    
    # Save the detailed data
    save_data(detailed_df)
