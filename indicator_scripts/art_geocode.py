import pandas as pd
import time
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- CONFIGURATION ---
input_files = {
    "England": Path(r"M:\Dissertation\indicators\venues_england.csv"),
    "Wales": Path(r"M:\Dissertation\indicators\venues_wales.csv")
}
output_filename = Path(r"M:\Dissertation\indicators\artuk_venues_geocoded.csv")
name_column = "Name"
# ---------------------

def geocode_dataframe(df, name_col, country_hint):
    """
    Takes a DataFrame and a country hint, adds 'latitude' and 'longitude'.
    """
    # --- TIMEOUT INCREASED HERE ---
    # Initialize the Nominatim geocoder with a longer timeout of 10 seconds.
    geolocator = Nominatim(user_agent="dissertation_geocoder_v3", timeout=10)

    # Use RateLimiter to ensure we don't send more than 1 request per second.
    # This is required by Nominatim's Terms of Service.
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1, error_wait_seconds=10, max_retries=2, swallow_exceptions=False)

    latitudes = []
    longitudes = []

    print(f"\n--- Starting geocoding for {country_hint} ---")
    print(f"Processing {len(df)} venues...")

    for index, row in df.iterrows():
        venue_name = row[name_col]
        # Create a more specific query by adding the country hint
        query = f"{venue_name}, {country_hint}"
        
        print(f"Processing {index + 1}/{len(df)}: Querying '{query}'")
        
        try:
            location = geocode(query)
            if location:
                latitudes.append(location.latitude)
                longitudes.append(location.longitude)
            else:
                latitudes.append(None)
                longitudes.append(None)
        except Exception as e:
            print(f"  -- An error occurred for query '{query}': {e}")
            latitudes.append(None)
            longitudes.append(None)

    df['latitude'] = latitudes
    df['longitude'] = longitudes
    df['country'] = country_hint # Add a country column for reference
    
    return df

def main():
    """
    Main function to read the files, process them, and save the combined results.
    """
    all_results = []

    for country, file_path in input_files.items():
        try:
            print(f"\nReading input file: {file_path}")
            df_to_process = pd.read_csv(file_path)
            df_to_process.dropna(subset=[name_column], inplace=True)
            
            geocoded_df = geocode_dataframe(df_to_process, name_column, country)
            all_results.append(geocoded_df)
            
        except FileNotFoundError:
            print(f"WARNING: Input file not found at '{file_path}'. Skipping.")
        except KeyError:
            print(f"WARNING: Column '{name_column}' not found in '{file_path}'. Skipping.")

    if not all_results:
        print("\nNo data was processed. Please check your input files and paths.")
        return

    final_df = pd.concat(all_results, ignore_index=True)

    try:
        final_df.to_csv(output_filename, index=False, encoding='utf-8')
        print(f"\nSuccess! Combined and geocoded data for {len(final_df)} venues saved to '{output_filename}'")
    except Exception as e:
        print(f"\nERROR: Could not save the output file. Reason: {e}")

if __name__ == "__main__":
    main()