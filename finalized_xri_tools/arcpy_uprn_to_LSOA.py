import os
import arcpy
import pandas as pd

# --- CONFIGURATION ---
# EDIT THESE PATHS TO MATCH YOUR SYSTEM

# The path to the CSV file containing your original address data with UPRNs.
# This MUST be the same file used to generate your tool outputs.
ADDRESSES_CSV = r"M:\Dissertation\fresh_geocodes.csv"

# The feature class of LSOA boundaries.
LSOA_BOUNDARIES = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"

# The field in the LSOA data that contains the unique LSOA code (e.g., "LSOA21CD").
LSOA_ID_FIELD = "LSOA21CD"

# The names of the columns in your addresses CSV for X and Y Coordinates.
X_FIELD = "X_COORDINATE"
Y_FIELD = "Y_COORDINATE"

# The full path for the final output CSV file with the address counts.
OUTPUT_CSV_PATH = r"M:\Dissertation\xRI_independent_tools\tool_outputs\lsoa_address_counts_arcpy.csv"
# ---------------------

def main():
    """
    Counts addresses per LSOA using ArcPy to ensure consistency with the main analysis.
    """
    print("Starting ArcPy-based LSOA address counter...")
    arcpy.env.overwriteOutput = True
    
    # Create output directory if it doesn't exist
    output_folder = os.path.dirname(OUTPUT_CSV_PATH)
    os.makedirs(output_folder, exist_ok=True)
    
    # Define a temporary CSV for cleaned data
    temp_cleaned_csv = os.path.join(arcpy.env.scratchFolder, "cleaned_addresses_for_count.csv")

    try:
        # Step 1: Clean the address data using the robust pandas method
        print("Step 1: Reading and cleaning address data...")
        df = pd.read_csv(ADDRESSES_CSV)
        
        # Force coordinate columns to be numeric, coercing errors to NaN
        df[X_FIELD] = pd.to_numeric(df[X_FIELD], errors='coerce')
        df[Y_FIELD] = pd.to_numeric(df[Y_FIELD], errors='coerce')
        
        # Drop rows with any invalid or missing coordinate values
        df.dropna(subset=[X_FIELD, Y_FIELD], inplace=True)
        
        print(f"Found {len(df)} addresses with valid coordinates.")
        df.to_csv(temp_cleaned_csv, index=False)

        # Step 2: Convert cleaned addresses to points
        print("Step 2: Creating point features from addresses...")
        temp_points = "in_memory/AddressPoints"
        arcpy.management.XYTableToPoint(
            in_table=temp_cleaned_csv,
            out_feature_class=temp_points,
            x_field=X_FIELD,
            y_field=Y_FIELD,
            coordinate_system=arcpy.SpatialReference(27700) # British National Grid
        )

        # Step 3: Perform a spatial join to count points within each LSOA
        print("Step 3: Performing spatial join to count addresses...")
        temp_join = "in_memory/LSOA_AddressJoin"
        arcpy.analysis.SpatialJoin(
            target_features=LSOA_BOUNDARIES,
            join_features=temp_points,
            out_feature_class=temp_join,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_COMMON",  # We only want LSOAs with addresses
            match_option="INTERSECT"
        )

        # Step 4: Export the results to a CSV file
        print("Step 4: Exporting results to CSV...")
        lsoa_counts = []
        # The 'Join_Count' field is automatically added by Spatial Join
        with arcpy.da.SearchCursor(temp_join, [LSOA_ID_FIELD, "Join_Count"]) as cursor:
            for row in cursor:
                lsoa_counts.append({'LSOA_Code': row[0], 'Address_Count': row[1]})
        
        if not lsoa_counts:
            print("\nWARNING: No addresses were found within any LSOA boundaries. Please check your data.")
            return

        # Convert to a DataFrame and save
        results_df = pd.DataFrame(lsoa_counts)
        results_df.to_csv(OUTPUT_CSV_PATH, index=False)

        print(f"\nSUCCESS: Script finished. Found {len(results_df)} LSOAs with addresses.")
        print(f"Output saved to: {OUTPUT_CSV_PATH}")

    except arcpy.ExecuteError:
        print("\nARCPY ERROR:")
        print(arcpy.GetMessages(2))
    except Exception as e:
        print(f"\nPYTHON ERROR: {e}")
    finally:
        # Clean up temporary data
        print("Cleaning up temporary files...")
        arcpy.Delete_management("in_memory")
        if os.path.exists(temp_cleaned_csv):
            os.remove(temp_cleaned_csv)

if __name__ == "__main__":
    main()