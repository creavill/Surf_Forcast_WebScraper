#!/usr/bin/env python3
"""
Main Script

This script orchestrates the entire data collection and merging process.
It runs each component in sequence to create a comprehensive surf break database.
"""

import os
import argparse
from datetime import datetime

# Import components
from scraper.break_list_scraper import scrape_surf_breaks, save_data as save_break_list
from scraper.break_detail_scraper import scrape_break_details, save_data as save_break_details
from data_processing.country_standardizer import standardize_countries_in_file
from data_processing.data_merger import merge_datasets, create_unmatched_datasets
from utils.cleaning_utils import create_directories


def setup_directories():
    """Create necessary directories for the project"""
    directories = ['data', 'logs', 'scraper', 'data_processing', 'utils']
    create_directories(directories)
    return directories


def run_pipeline(scrape_breaks=True, scrape_details=True, standardize=True, merge=True, second_source=None):
    """
    Run the complete data pipeline.
    
    Args:
        scrape_breaks (bool): Whether to scrape the list of breaks
        scrape_details (bool): Whether to scrape break details
        standardize (bool): Whether to standardize country names
        merge (bool): Whether to merge datasets
        second_source (str): Path to second source data file (optional)
    """
    # Setup directories
    setup_directories()
    
    # Define file paths
    data_dir = "data"
    breaks_list_path = os.path.join(data_dir, "surf_breaks_list.csv")
    breaks_complete_path = os.path.join(data_dir, "surf_breaks_complete.csv")
    breaks_standardized_path = os.path.join(data_dir, "surf_breaks_complete_standardized.csv")
    
    second_source_standardized_path = os.path.join(data_dir, "additional_source_complete_standardized.csv")
    merged_path = os.path.join(data_dir, "merged_surf_breaks.csv")
    
    # Start pipeline
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== Starting surf data pipeline at {timestamp} ===")
    
    # Step 1: Scrape list of breaks
    if scrape_breaks:
        print("\n=== Step 1: Scraping list of surf breaks ===")
        breaks_df = scrape_surf_breaks()
        save_break_list(breaks_df, output_dir=data_dir, filename="surf_breaks_list.csv")
    else:
        print("\n=== Step 1: Skipping scraping of surf breaks list ===")
    
    # Step 2: Scrape break details
    if scrape_details:
        print("\n=== Step 2: Scraping detailed break information ===")
        if not os.path.exists(breaks_list_path):
            print(f"Error: Breaks list file '{breaks_list_path}' not found!")
            return
        
        breaks_df = scrape_break_details(pd.read_csv(breaks_list_path))
        save_break_details(breaks_df, output_dir=data_dir, filename="surf_breaks_complete.csv")
    else:
        print("\n=== Step 2: Skipping scraping of break details ===")
    
    # Step 3: Standardize country names
    if standardize:
        print("\n=== Step 3: Standardizing country names ===")
        if not os.path.exists(breaks_complete_path):
            print(f"Error: Breaks complete file '{breaks_complete_path}' not found!")
            return
        
        standardize_countries_in_file(breaks_complete_path, breaks_standardized_path)
        
        # If a second source was provided, standardize it too
        if second_source and os.path.exists(second_source):
            second_source_name = os.path.basename(second_source)
            print(f"Standardizing second source: {second_source_name}")
            standardize_countries_in_file(second_source, second_source_standardized_path)
    else:
        print("\n=== Step 3: Skipping country name standardization ===")
    
    # Step 4: Merge datasets
    if merge:
        print("\n=== Step 4: Merging datasets ===")
        if not os.path.exists(breaks_standardized_path):
            print(f"Error: Standardized breaks file '{breaks_standardized_path}' not found!")
            return
        
        if second_source and os.path.exists(second_source_standardized_path):
            print("Merging with second source data...")
            merge_stats = merge_datasets(breaks_standardized_path, second_source_standardized_path, merged_path)
            
            # Print merge statistics
            print("\nMerge Statistics:")
            for key, value in merge_stats.items():
                print(f"  {key}: {value}")
            
            # Create datasets of unmatched entries
            create_unmatched_datasets(breaks_standardized_path, second_source_standardized_path, merged_path, data_dir)
        else:
            print("No second source data found. Using only the primary source.")
            # Copy breaks_standardized to merged_path
            import shutil
            shutil.copy(breaks_standardized_path, merged_path)
    else:
        print("\n=== Step 4: Skipping dataset merging ===")
    
    # Pipeline complete
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n=== Surf data pipeline completed at {timestamp} ===")


if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Surf Break Data Pipeline")
    parser.add_argument("--skip-breaks", action="store_true", help="Skip scraping list of breaks")
    parser.add_argument("--skip-details", action="store_true", help="Skip scraping break details")
    parser.add_argument("--skip-standardize", action="store_true", help="Skip standardizing country names")
    parser.add_argument("--skip-merge", action="store_true", help="Skip merging datasets")
    parser.add_argument("--second-source", type=str, help="Path to second source data file")
    
    # Parse arguments
    args = parser.parse_args()
    
    # Run pipeline with parsed arguments
    run_pipeline(
        scrape_breaks=not args.skip_breaks,
        scrape_details=not args.skip_details,
        standardize=not args.skip_standardize,
        merge=not args.skip_merge,
        second_source=args.second_source
    )
