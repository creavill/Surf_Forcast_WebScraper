#!/usr/bin/env python3
"""
Data Merger Module

This module merges data from multiple sources based on standardized country names
and surf break names, creating a comprehensive dataset of surf spots.
"""

import os
import string
import pandas as pd


def clean_name(name):
    """
    Clean a surf break name for comparison.
    
    Args:
        name (str): Surf break name to clean
        
    Returns:
        str: Cleaned surf break name
    """
    # Create a translation table to remove punctuation
    trans = str.maketrans('', '', string.punctuation)
    
    # Clean the name by removing punctuation, spaces, and converting to lowercase
    return str(name).translate(trans).replace(' ', '').lower()


def merge_datasets(source1_path, source2_path, output_path):
    """
    Merge two surf break datasets.
    
    Args:
        source1_path (str): Path to first source CSV
        source2_path (str): Path to second source CSV
        output_path (str): Path to output merged CSV
    """
    print(f"Merging datasets from {source1_path} and {source2_path}...")
    
    # Load the CSV files
    source1 = pd.read_csv(source1_path)
    source2 = pd.read_csv(source2_path)
    
    # Make a copy of the name column for source1 as 'Alternative name'
    source1['Alternative name'] = source1['name']
    
    # Clean names for matching
    source1['clean_name'] = source1['name'].apply(clean_name)
    source2['clean_name'] = source2['name'].apply(clean_name)
    
    # Clean alternative names if they exist in source2
    if 'Alternative name' in source2.columns:
        source2['clean_alt_name'] = source2['Alternative name'].apply(clean_name)
    else:
        source2['Alternative name'] = source2['name']
        source2['clean_alt_name'] = source2['clean_name']
    
    # Merge datasets based on clean name and country
    print("Performing initial merge on clean name and country...")
    merged_df = pd.merge(
        source1, 
        source2,
        on=['clean_name', 'country'], 
        how='inner',
        suffixes=('_source1', '_source2')
    )
    
    # Handle remaining entries - check for matches between name and alternative name
    source1_leftover = source1[~source1['clean_name'].isin(merged_df['clean_name'])]
    source2_leftover = source2[~source2['clean_name'].isin(merged_df['clean_name'])]
    
    print(f"Found {len(merged_df)} direct matches. Processing remaining entries...")
    print(f"Source 1 leftovers: {len(source1_leftover)}")
    print(f"Source 2 leftovers: {len(source2_leftover)}")
    
    # Try matching source1 names with source2 alternative names
    name_alt_matches = pd.merge(
        source1_leftover,
        source2_leftover,
        left_on=['clean_name', 'country'],
        right_on=['clean_alt_name', 'country'],
        how='inner',
        suffixes=('_source1', '_source2')
    )
    
    # Try matching source1 alternative names with source2 names
    alt_name_matches = pd.merge(
        source1_leftover,
        source2_leftover,
        left_on=['clean_alt_name', 'country'],
        right_on=['clean_name', 'country'],
        how='inner',
        suffixes=('_source1', '_source2')
    )
    
    print(f"Found {len(name_alt_matches)} name-to-alternative matches.")
    print(f"Found {len(alt_name_matches)} alternative-to-name matches.")
    
    # Combine all matches
    all_matches = pd.concat([merged_df, name_alt_matches, alt_name_matches], ignore_index=True)
    
    # Drop temporary columns used for matching
    columns_to_drop = ['clean_name', 'clean_alt_name']
    all_matches = all_matches.drop(columns=[col for col in columns_to_drop if col in all_matches.columns])
    
    # Save the merged dataset
    all_matches.to_csv(output_path, index=False)
    print(f"Merged data saved to {output_path}")
    print(f"Total entries in merged dataset: {len(all_matches)}")
    
    # Calculate and return statistics about the merge
    return {
        'total_merged': len(all_matches),
        'direct_matches': len(merged_df),
        'name_alt_matches': len(name_alt_matches),
        'alt_name_matches': len(alt_name_matches),
        'source1_unmatched': len(source1) - len(all_matches),
        'source2_unmatched': len(source2) - len(all_matches)
    }


def create_unmatched_datasets(source1_path, source2_path, merged_path, output_dir):
    """
    Create datasets of unmatched entries for further analysis.
    
    Args:
        source1_path (str): Path to first source CSV
        source2_path (str): Path to second source CSV
        merged_path (str): Path to merged CSV
        output_dir (str): Directory to save unmatched datasets
    """
    # Load datasets
    source1 = pd.read_csv(source1_path)
    source2 = pd.read_csv(source2_path)
    merged = pd.read_csv(merged_path)
    
    # Clean names for matching
    source1['clean_name'] = source1['name'].apply(clean_name)
    source2['clean_name'] = source2['name'].apply(clean_name)
    merged['clean_name'] = merged['name_source1'].apply(clean_name)
    
    # Find unmatched entries
    source1_unmatched = source1[~source1['clean_name'].isin(merged['clean_name'])]
    source2_unmatched = source2[~source2['clean_name'].isin(merged['clean_name'])]
    
    # Drop temporary columns
    source1_unmatched = source1_unmatched.drop(columns=['clean_name'])
    source2_unmatched = source2_unmatched.drop(columns=['clean_name'])
    
    # Save unmatched datasets
    source1_unmatched_path = os.path.join(output_dir, "source1_unmatched.csv")
    source2_unmatched_path = os.path.join(output_dir, "source2_unmatched.csv")
    
    source1_unmatched.to_csv(source1_unmatched_path, index=False)
    source2_unmatched.to_csv(source2_unmatched_path, index=False)
    
    print(f"Source 1 unmatched entries saved to {source1_unmatched_path}")
    print(f"Source 2 unmatched entries saved to {source2_unmatched_path}")


def main():
    """Main function to run the data merger"""
    # Create data directory if it doesn't exist
    data_dir = "../data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Define input and output files
    source1_path = os.path.join(data_dir, "surf_breaks_complete_standardized.csv")
    source2_path = os.path.join(data_dir, "additional_source_complete_standardized.csv")
    merged_path = os.path.join(data_dir, "merged_surf_breaks.csv")
    
    # Check if input files exist
    if not os.path.exists(source1_path):
        print(f"Input file {source1_path} not found!")
        print("Please run country_standardizer.py first.")
        return
    
    if not os.path.exists(source2_path):
        print(f"Second source file {source2_path} not found!")
        print("Using only the first source data.")
        # Copy source1 to merged_path
        source1 = pd.read_csv(source1_path)
        source1.to_csv(merged_path, index=False)
        print(f"Single source data saved to {merged_path}")
        return
    
    # Merge the datasets
    merge_stats = merge_datasets(source1_path, source2_path, merged_path)
    
    # Print merge statistics
    print("\nMerge Statistics:")
    for key, value in merge_stats.items():
        print(f"  {key}: {value}")
    
    # Create datasets of unmatched entries
    create_unmatched_datasets(source1_path, source2_path, merged_path, data_dir)


if __name__ == "__main__":
    main()
