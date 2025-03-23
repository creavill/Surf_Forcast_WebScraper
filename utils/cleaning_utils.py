#!/usr/bin/env python3
"""
Cleaning Utilities Module

This module provides utility functions for cleaning and processing data
used across multiple scripts in the project.
"""

import os
import string
import pandas as pd


def create_directories(dirs):
    """
    Create multiple directories if they don't exist.
    
    Args:
        dirs (list): List of directory paths to create
    """
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)


def clean_text(text, remove_spaces=False, lowercase=True):
    """
    Clean text by removing punctuation and optionally spaces and case.
    
    Args:
        text (str): Text to clean
        remove_spaces (bool): Whether to remove spaces
        lowercase (bool): Whether to convert to lowercase
        
    Returns:
        str: Cleaned text
    """
    # Convert to string in case of non-string input
    text = str(text)
    
    # Create a translation table to remove punctuation
    trans = str.maketrans('', '', string.punctuation)
    
    # Remove punctuation
    text = text.translate(trans)
    
    # Remove spaces if requested
    if remove_spaces:
        text = text.replace(' ', '')
    
    # Convert to lowercase if requested
    if lowercase:
        text = text.lower()
    
    return text


def compare_column_values(df1, df2, column, clean=True):
    """
    Compare values in a column between two DataFrames.
    
    Args:
        df1 (pd.DataFrame): First DataFrame
        df2 (pd.DataFrame): Second DataFrame
        column (str): Column name to compare
        clean (bool): Whether to clean values before comparison
        
    Returns:
        tuple: (common_values, unique_values) as lists
    """
    # Get unique values from each DataFrame
    values1 = df1[column].unique()
    values2 = df2[column].unique()
    
    # Clean values if requested
    if clean:
        values1 = [clean_text(v) for v in values1]
        values2 = [clean_text(v) for v in values2]
    
    # Find common and unique values
    common_values = list(set(values1) & set(values2))
    unique_values = list(set(values1) ^ set(values2))
    
    return common_values, unique_values


def find_duplicates(df, columns=None, return_counts=False):
    """
    Find duplicate entries in a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to check for duplicates
        columns (list): Columns to consider for duplicates (None = all columns)
        return_counts (bool): Whether to return counts of duplicates
        
    Returns:
        pd.DataFrame: Duplicate entries
    """
    if columns is None:
        duplicates = df[df.duplicated(keep=False)]
    else:
        duplicates = df[df.duplicated(subset=columns, keep=False)]
    
    if return_counts:
        if columns is None:
            counts = df.groupby(list(df.columns)).size().reset_index(name='count')
            counts = counts[counts['count'] > 1]
        else:
            counts = df.groupby(columns).size().reset_index(name='count')
            counts = counts[counts['count'] > 1]
        return duplicates, counts
    
    return duplicates


def standardize_column_names(df):
    """
    Standardize column names in a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame with columns to standardize
        
    Returns:
        pd.DataFrame: DataFrame with standardized column names
    """
    # Create a copy of the DataFrame
    df_clean = df.copy()
    
    # Clean column names: lowercase, replace spaces with underscores
    df_clean.columns = [
        col.lower().replace(' ', '_').replace('-', '_') for col in df_clean.columns
    ]
    
    return df_clean


def remove_empty_columns(df, threshold=0.9):
    """
    Remove columns that have a high percentage of empty values.
    
    Args:
        df (pd.DataFrame): DataFrame to process
        threshold (float): Threshold for percentage of empty values (0.0-1.0)
        
    Returns:
        pd.DataFrame: DataFrame with empty columns removed
    """
    # Calculate percentage of empty values for each column
    empty_pct = df.isna().mean()
    
    # Get columns to keep
    cols_to_keep = empty_pct[empty_pct < threshold].index.tolist()
    
    return df[cols_to_keep]