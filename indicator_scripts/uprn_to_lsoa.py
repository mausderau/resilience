import pandas as pd
import geopandas as gpd

# --- CONFIGURATION ---
# IMPORTANT: Replace these file paths with the correct paths on your system.
# Path to your CSV file containing the original address data with UPRNs and coordinates.
# This CSV must have columns for a unique ID (e.g., UPRN), Latitude, and Longitude.
addresses_csv_path = r"M:\Dissertation\fresh_geocodes.csv"

# Path to your LSOA boundaries shapefile (.shp).
lsoa_shapefile_path = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"

# The name of the column in your addresses CSV that contains the unique address ID (e.g., UPRN).
# Make sure to replace 'UPRN' if your column has a different name.
address_id_column = 'UPRN'

# The names of the columns in your addresses CSV for Latitude and Longitude.
lat_column = 'X_COORDINATE'
lon_column = 'Y_COORDINATE'

# The name of the LSOA code column in your shapefile.
lsoa_id_column = "LSOA21CD"

# The name of the output CSV file where the address counts will be saved.
output_csv_path = "lsoa_address_counts.csv"
# ---------------------


def count_addresses_by_lsoa():
    """
    Performs a spatial join to count the number of addresses within each LSOA.
    """
    print("Step 1: Reading LSOA boundaries shapefile...")
    try:
        # Read the LSOA boundaries shapefile into a GeoDataFrame
        lsoa_gdf = gpd.read_file(lsoa_shapefile_path)
    except Exception as e:
        print(f"Error reading shapefile: {e}")
        return

    print("Step 2: Reading address data from CSV...")
    try:
        # Read the addresses CSV into a pandas DataFrame
        addresses_df = pd.read_csv(addresses_csv_path)

        # --- ADD THESE LINES ---
        # Force coordinate columns to be numeric, coercing errors to NaN
        addresses_df[lat_column] = pd.to_numeric(addresses_df[lat_column], errors='coerce')
        addresses_df[lon_column] = pd.to_numeric(addresses_df[lon_column], errors='coerce')

        # Now, drop any rows with missing/invalid coordinates
        addresses_df.dropna(subset=[lat_column, lon_column], inplace=True)
    except Exception as e:
        print(f"Error reading or cleaning CSV: {e}")
        return

    print("Step 3: Converting addresses to a GeoDataFrame...")
    # Create a GeoDataFrame from the addresses DataFrame
    # The 'geometry' column is created from the lat/lon coordinates.
    addresses_gdf = gpd.GeoDataFrame(
        addresses_df,
        geometry=gpd.points_from_xy(addresses_df[lon_column], addresses_df[lat_column]),
        crs="epsg:27700"
    )

    print("Step 4: Performing spatial join...")
    # Perform a spatial join between the addresses and LSOA boundaries.
    # The 'inner' join type ensures that only addresses that fall within an LSOA are kept.
    # The 'within' predicate checks if each address point is located within an LSOA polygon.
    joined_gdf = gpd.sjoin(
        addresses_gdf,
        lsoa_gdf[[lsoa_id_column, 'geometry']],
        how='inner',
        predicate='within'
    )
    
    # Check if any addresses were joined
    if joined_gdf.empty:
        print("No addresses were found within the LSOA boundaries. Please check your file paths and coordinate system.")
        return

    print("Step 5: Counting unique addresses per LSOA...")
    # Group the joined GeoDataFrame by LSOA code and count the unique address IDs.
    lsoa_counts = joined_gdf.groupby(lsoa_id_column)[address_id_column].nunique()

    # Convert the Series to a DataFrame and rename the count column
    lsoa_counts_df = lsoa_counts.reset_index(name='Address_Count')
    
    print(f"Number of LSOAs with addresses found: {len(lsoa_counts_df)}")
    print("Here are the first 5 results:")
    print(lsoa_counts_df.head())

    print(f"\nStep 6: Saving results to '{output_csv_path}'...")
    # Save the final DataFrame to a new CSV file
    lsoa_counts_df.to_csv(output_csv_path, index=False)
    print("Script finished successfully!")

if __name__ == "__main__":
    count_addresses_by_lsoa()
