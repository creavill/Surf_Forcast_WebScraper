#!/usr/bin/env python3
"""
Break List Scraper Module

This module scrapes a list of surf breaks from a surf forecast website,
collecting basic information such as break names, links, and countries.
"""

import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm


def scrape_surf_breaks(pages=27):
    """
    Scrape the list of surf breaks from the surf forecast website.
    
    Args:
        pages (int): Number of pages to scrape
        
    Returns:
        pd.DataFrame: DataFrame containing break names, links, and countries
    """
    print("Scraping surf break list...")
    base_url = "PLACEHOLDER_URL/breaks?page="  # URL placeholder to be replaced with actual site
    data = []

    for i in tqdm(range(1, pages + 1), desc="Scraping pages"):
        url = base_url + str(i)
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            
            soup = BeautifulSoup(response.text, 'html.parser')
            rows = soup.find_all('td')

            for col in rows:
                a_tag = col.find('a')
                span_tag = col.find('span', class_='rem')
                if a_tag and span_tag:
                    name = a_tag.text
                    link = a_tag['href']
                    country = span_tag.text
                    data.append({
                        'name': name,
                        'link': link,
                        'country': country
                    })
        except requests.exceptions.RequestException as e:
            print(f"Error scraping page {i}: {e}")
            continue

    # Create DataFrame from collected data
    df = pd.DataFrame(data)
    print(f"Successfully scraped {len(df)} surf breaks")
    
    return df


def save_data(df, output_dir="../data", filename="surf_breaks_list.csv"):
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
    print(f"Data saved to {output_path}")


if __name__ == "__main__":
    # Scrape surf breaks
    breaks_df = scrape_surf_breaks()
    
    # Save data
    save_data(breaks_df)
