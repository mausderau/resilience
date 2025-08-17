import pandas as pd
import os

# --- Define File Paths ---

# The main spreadsheet you provided
main_indicators_csv = r"M:\Dissertation\indicators\total_indicators.csv" 

# The new CSV you just exported from ArcGIS Pro
script_outputs_csv = r"M:\Dissertation\indicators\indicators_lsoa.csv"

# The final, combined master file you want to create
output_folder = r"M:\Dissertation\indicators\output"
master_csv_path = os.path.join(output_folder, "master_indicators.csv")


# --- The Join Key ---

# This is the name of the column that uniquely identifies each LSOA.
# **IMPORTANT**: Make sure this column name is exactly the same in both CSV files.
join_key = "LSOA21CD"


# --- Load and Join the Data ---

try:
    print("Loading datasets...")
    # Load your main spreadsheet into a pandas DataFrame
    df_main = pd.read_csv(main_indicators_csv)
    
    # Load the results from your scripts into another DataFrame
    df_script = pd.read_csv(script_outputs_csv)
    
    print(f"Joining files on the key: '{join_key}'...")
    # Merge the two DataFrames using a "left" join.
    # This ensures all rows from your main spreadsheet are kept.
    master_df = pd.merge(df_main, df_script, on=join_key, how='left')
    
    # Save the final, combined DataFrame to a new CSV file
    master_df.to_csv(master_csv_path, index=False)
    
    print("\n✅ Success!")
    print(f"Master indicator file created at: {master_csv_path}")

except FileNotFoundError as e:
    print(f"\n❌ ERROR: A file was not found. Please check your file paths.")
    print(e)
except KeyError as e:
    print(f"\n❌ ERROR: The join key '{join_key}' was not found in one of the files.")
    print("Please ensure the LSOA ID column has the exact same name in both CSV files.")
    print(e)