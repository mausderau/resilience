import geopandas as gpd
import pandas as pd

# --- Input file paths ---
lsoa_shapefile = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"
roads_shapefile = r"M:\Dissertation\indicators\aggregate_to_lsoa\major_roads\Major_Road_Network_2018_Open_Roads.shp"
population_csv = r"M:\Dissertation\indicators\aggregate_to_lsoa\population_count_lsoa.csv"

# Columns
lsoa_code_col = "LSOA21CD"       # adjust to match your LSOA shapefile
population_code_col = "LSOA21CD" # adjust to match your population CSV
population_col = "population"    # adjust to match your CSV

# --- Load data ---
lsoas = gpd.read_file(lsoa_shapefile)
roads = gpd.read_file(roads_shapefile)
population = pd.read_csv(population_csv)

# --- Reproject everything to a projected CRS in meters ---
# Use British National Grid if UK data (EPSG:27700)
lsoas = lsoas.to_crs(epsg=27700)
roads = roads.to_crs(epsg=27700)

# --- Spatial join: intersect roads with LSOAs ---
road_lsoa = gpd.overlay(roads, lsoas, how="intersection")

# --- Calculate road lengths ---
road_lsoa["road_length_m"] = road_lsoa.geometry.length

# --- Sum road length per LSOA ---
road_length_per_lsoa = road_lsoa.groupby(lsoa_code_col)["road_length_m"].sum().reset_index()

# --- Merge with population ---
df = lsoas[[lsoa_code_col]].merge(road_length_per_lsoa, on=lsoa_code_col, how="left")
df = df.merge(population[[population_code_col, population_col]],
              left_on=lsoa_code_col, right_on=population_code_col,
              how="left")

# Fill missing road length with 0
df["road_length_m"] = df["road_length_m"].fillna(0)

# --- Calculate road per capita ---
df["m_per_capita"] = df["road_length_m"] / df[population_col]

# --- Save results ---
df.to_csv("lsoa_major_road_per_capita.csv", index=False)

print("✅ Done! Results saved to lsoa_major_road_per_capita.csv")
