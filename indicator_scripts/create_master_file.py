# File: create_master_file.py

import pandas as pd
import os

# --- CONFIGURATION ---
# The original file from your free Nominatim run
NOMINATIM_FILE = r"M:\Dissertation\indicators\geocoded_progress_address.csv"
# The file that contains the partial results from your Google run
GOOGLE_FILE = r"M:\Dissertation\indicators\google_charity_input2.csv"
# The name of the new, combined master file we will create
MASTER_PROGRESS_FILE = r"M:\Dissertation\indicators\charity_wgs.csv"

# --- SCRIPT ---
if not os.path.exists(NOMINATIM_FILE) or not os.path.exists(GOOGLE_FILE):
    print("Error: One or both source files are missing. Cannot create master file.")
    exit()

print("Loading data files...")
master_df = pd.read_csv(NOMINATIM_FILE)
google_df = pd.read_csv(GOOGLE_FILE)

# --- NEW: Remove Duplicates ---
print("Removing duplicate addresses to ensure a clean merge...")
# This removes any rows that have the exact same 'full_address', keeping the first one it finds.
master_df.drop_duplicates(subset=['full_address'], keep='first', inplace=True)
google_df.drop_duplicates(subset=['full_address'], keep='first', inplace=True)
# --- End of New Code ---

print("Merging all saved progress...")
master_df.set_index('full_address', inplace=True)
google_df.set_index('full_address', inplace=True)

master_df.update(google_df)

master_df.reset_index(inplace=True)

master_df.to_csv(MASTER_PROGRESS_FILE, index=False)

print(f"\nSuccessfully created master progress file: '{MASTER_PROGRESS_FILE}'")
print("This file now contains all your completed work from both Nominatim and Google.")
