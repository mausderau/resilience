import pandas as pd
import geopandas as gpd
import os
import time

def main():
    start_time = time.time()
    print("Starting FINAL GeoPandas master analysis script...")

    # --- (1) USER-DEFINED PARAMETERS ---
    # **IMPORTANT**: Please verify all paths and field names below.

    # -- Paths --
    lsoa_shp_path = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"
    output_folder = r"M:\Dissertation\indicators\output"
    final_output_gpkg = os.path.join(output_folder, "Master_Indicators_GeoPandas_Final.gpkg")
    population_data_csv = r"M:\Dissertation\indicators\aggregate_to_lsoa\population_count_lsoa.csv"

    # -- Data Sources --
    # BNG = "EPSG:27700", WGS84 = "EPSG:4326"
    data_sources = {
        "art_venues": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\art_venues_geocoded_real.csv", "x": "longitude", "y": "latitude", "crs": "EPSG:4326"},
        "naptan_stops": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\naptan_stops_bng\naptan_stops_bng.shp"},
        "loans": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\lending_bng.csv", "x": "X", "y": "Y", "crs": "EPSG:27700"},
        "special_sites": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\special_sites_bng_2.csv", "x": "X", "y": "Y", "crs": "EPSG:27700"},
        "landfill": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\Historic_Landfill_Sites\Historic_Landfill_SitesPolygon.shp"},
        "retail": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\retail_fix.shp"},
        "nuclear": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\nuclear_sites.csv", "x": "Longitude", "y": "Latitude", "crs": "EPSG:4326"},
        "flood_risk": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\flood_risk_areas\data\Flood_Risk_Areas.shp"},
        "radon": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\radon_indicative_atlas\Radon_Indicative_Atlas_v3.shp"},
        "greenspace": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\greenspace\GreenspaceSite_Merge.shp"},
        "major_roads": {"path": r"M:\Dissertation\indicators\aggregate_to_lsoa\major_roads\Major_Road_Network_2018_Open_Roads.shp"}
    }
    
    # -- Field Names & Weighting Schemes --
    lsoa_join_field = "LSOA21CD"
    population_field_in_csv = "population"
    landfill_dissolve_field = "site_name"
    special_sites_area_field = "USER_Land_area__ha_"
    nuclear_status_field = "Status"
    retail_class_field = "Classifica"
    flood_risk_class_field = "frr_cycle"
    radon_class_field = "CLASS_MAX"
    
    nuclear_weights = {
        "high": ['Operational', 'Defuelling', 'Under Construction', 'Proposed New Build'],
        "medium": ['Decommissioning', 'Under Decommissioning', 'Permanent Shutdown']
    }
    retail_weights = {
        'Small Local Centre': 1, 'Local Centre': 2, 'Small Retail Park': 2, 'Small Shopping Centre': 2,
        'District Centre': 3, 'Town Centre': 3, 'Market Town': 3, 'Large Shopping Centre': 3,
        'Large Retail Park': 4, 'Major Town Centre': 4,
        'Regional Centre': 5
    }
    flood_weights = {1: 1.0, 2: 2.0}
    radon_weights = {3: 3.0, 4: 4.0, 5: 5.0, 6: 6.0}

    # --- (2) DATA LOADING AND PREPARATION ---
    print("\nLoading and preparing LSOA data...")
    bng_crs = "EPSG:27700"
    lsoas = gpd.read_file(lsoa_shp_path).to_crs(bng_crs)
    lsoas['lsoa_area_sqkm'] = lsoas.geometry.area / 1_000_000
    
    print("Creating 100m neighbourhood boundaries for LSOAs...")
    lsoa_neighbourhoods = lsoas.copy()
    lsoa_neighbourhoods['geometry'] = lsoas.geometry.buffer(100)

    indicators = {}

    def load_geodataframe(params):
        if "x" in params:
            df = pd.read_csv(params["path"])
            gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[params["x"]], df[params["y"]]), crs=params["crs"])
        else:
            gdf = gpd.read_file(params["path"], layer=params.get("layer"))
        return gdf.to_crs(bng_crs)

    # --- (3) INDICATOR CALCULATION ---

    print("\n--- Processing Neighbourhood Indicators ---")
    
    # Simple Counts (Art Venues, Naptan Stops, Major Roads)
    for name, out_field in [("art_venues", "ArtVenue_Nabe"), ("naptan_stops", "Naptan_Nabe"), ("major_roads", "MajorRoad_Nabe")]:
        print(f"  Calculating: {name} count...")
        gdf = load_geodataframe(data_sources[name])
        join = gpd.sjoin(gdf, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
        indicators[out_field] = join.groupby(lsoa_join_field).size()

    # Simple Sums (Loans, Special Sites)
    print("  Calculating: loans sum...")
    loans = load_geodataframe(data_sources["loans"])
    loans_join = gpd.sjoin(loans, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    indicators["LoanValue_Nabe"] = loans_join.groupby(lsoa_join_field)['Loan_21_24'].sum()
    
    print("  Calculating: special sites area sum...")
    special_sites = load_geodataframe(data_sources["special_sites"])
    sites_join = gpd.sjoin(special_sites, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    indicators["SpcSiteArea_Nabe"] = sites_join.groupby(lsoa_join_field)[special_sites_area_field].sum()

    # Weighted Counts (Landfill, Retail, Nuclear)
    print("  Calculating: landfill count (by site)...")
    landfills = load_geodataframe(data_sources["landfill"]).dissolve(by=landfill_dissolve_field)
    landfill_join = gpd.sjoin(landfills, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    indicators["Landfill_Nabe"] = landfill_join.groupby(lsoa_join_field).size()
    
    print("  Calculating: retail weighted access...")
    retail = load_geodataframe(data_sources["retail"])
    retail['RetailWeight'] = retail[retail_class_field].map(retail_weights).fillna(0)
    retail_join = gpd.sjoin(retail, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    indicators["RetailAccess_Wt"] = retail_join.groupby(lsoa_join_field)['RetailWeight'].sum()
    
    print("  Calculating: nuclear weighted exposure...")
    nuclear = load_geodataframe(data_sources["nuclear"])
    high_risk_nuclear = nuclear[nuclear[nuclear_status_field].isin(nuclear_weights['high'])]
    med_risk_nuclear = nuclear[nuclear[nuclear_status_field].isin(nuclear_weights['medium'])]
    high_join = gpd.sjoin(high_risk_nuclear, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    med_join = gpd.sjoin(med_risk_nuclear, lsoa_neighbourhoods[[lsoa_join_field, 'geometry']], how="inner", predicate="intersects")
    high_counts = high_join.groupby(lsoa_join_field).size()
    med_counts = med_join.groupby(lsoa_join_field).size()
    indicators["NukeExp_Weighted"] = (high_counts * 1.0).add(med_counts * 0.5, fill_value=0)

    print("\n--- Processing Percent Area Indicators ---")
    
    # --- Greenspace Percent Area ---
    print("  Calculating: greenspace percent area...")
    greenspace_cache_path = os.path.join(output_folder, "cache_indicator_Green_Pct.csv")
    
    if os.path.exists(greenspace_cache_path):
        print("    Found cached result, loading from file...")
        indicators["Green_Pct"] = pd.read_csv(greenspace_cache_path, index_col=lsoa_join_field).squeeze("columns")
    else:
        greenspace = load_geodataframe(data_sources["greenspace"])
        print("    Step 1/3: Dissolving greenspace layer...")
        greenspace_dissolved = greenspace.dissolve()
        print("    Step 2/3: Intersecting with LSOAs (this is the slowest step)...")
        intersection = gpd.overlay(lsoas, greenspace_dissolved, how="intersection")
        print("    Step 3/3: Summarizing results...")
        intersection['piece_area'] = intersection.geometry.area
        intersection_area_sum = intersection.groupby(lsoa_join_field)['piece_area'].sum()
        indicators["Green_Pct"] = (intersection_area_sum / (lsoas.set_index(lsoa_join_field)['lsoa_area_sqkm'] * 1_000_000)) * 100
        print(f"    Saving result to cache file: {os.path.basename(greenspace_cache_path)}")
        indicators["Green_Pct"].to_csv(greenspace_cache_path, header=True, index_label=lsoa_join_field)
    print("    Greenspace calculation complete.")

    # --- Flood Risk (calculating average risk level) ---
    print("\n  Calculating: flood risk average level...")
    flood_cache_path = os.path.join(output_folder, "cache_indicator_FloodRisk_AvgLevel.csv")

    if os.path.exists(flood_cache_path):
        print("    Found cached result, loading from file...")
        indicators["FloodRisk_AvgLevel"] = pd.read_csv(flood_cache_path, index_col=lsoa_join_field).squeeze("columns")
    else:
        flood = load_geodataframe(data_sources["flood_risk"])
        flood_numerator = pd.Series(0.0, index=lsoas.index)
        flood_denominator = pd.Series(0.0, index=lsoas.index)
        for risk_val, weight in flood_weights.items():
            print(f"    Processing Flood Risk Class {risk_val} (this may be slow)...")
            class_subset = flood[flood[flood_risk_class_field] == risk_val].dissolve()
            if not class_subset.empty:
                intersection = gpd.overlay(lsoas, class_subset, how="intersection")
                intersection['piece_area'] = intersection.geometry.area
                intersection_area_sum = intersection.groupby(lsoa_join_field)['piece_area'].sum()
                pct_area = (intersection_area_sum / (lsoas.set_index(lsoa_join_field)['lsoa_area_sqkm'] * 1_000_000)) * 100
                flood_numerator = flood_numerator.add(pct_area * weight, fill_value=0)
                flood_denominator = flood_denominator.add(pct_area, fill_value=0)
        indicators["FloodRisk_AvgLevel"] = flood_numerator / flood_denominator
        print(f"    Saving result to cache file: {os.path.basename(flood_cache_path)}")
        indicators["FloodRisk_AvgLevel"].to_csv(flood_cache_path, header=True, index_label=lsoa_join_field)
    print("    Finished processing all flood risk classes.")

    # --- Radon (calculating average risk level) ---
    print("\n  Calculating: radon risk average level...")
    radon_cache_path = os.path.join(output_folder, "cache_indicator_RadonRisk_AvgLevel.csv")
    
    if os.path.exists(radon_cache_path):
        print("    Found cached result, loading from file...")
        indicators["RadonRisk_AvgLevel"] = pd.read_csv(radon_cache_path, index_col=lsoa_join_field).squeeze("columns")
    else:
        radon = load_geodataframe(data_sources["radon"])
        radon_numerator = pd.Series(0.0, index=lsoas.index)
        radon_denominator = pd.Series(0.0, index=lsoas.index)
        for risk_val, weight in radon_weights.items():
            print(f"    Processing Radon Risk Class {risk_val} (this may be slow)...")
            class_subset = radon[radon[radon_class_field] == risk_val].dissolve()
            if not class_subset.empty:
                intersection = gpd.overlay(lsoas, class_subset, how="intersection")
                intersection['piece_area'] = intersection.geometry.area
                intersection_area_sum = intersection.groupby(lsoa_join_field)['piece_area'].sum()
                pct_area = (intersection_area_sum / (lsoas.set_index(lsoa_join_field)['lsoa_area_sqkm'] * 1_000_000)) * 100
                radon_numerator = radon_numerator.add(pct_area * weight, fill_value=0)
                radon_denominator = radon_denominator.add(pct_area, fill_value=0)
        indicators["RadonRisk_AvgLevel"] = radon_numerator / radon_denominator
        print(f"    Saving result to cache file: {os.path.basename(radon_cache_path)}")
        indicators["RadonRisk_AvgLevel"].to_csv(radon_cache_path, header=True, index_label=lsoa_join_field)
    print("    Finished processing all radon risk classes.")

    # --- (4) COMBINE AND SAVE ---
    print("\nCombining all indicators...")
    final_gdf = lsoas
    for name, series in indicators.items():
        final_gdf = final_gdf.merge(series.rename(name), left_on=lsoa_join_field, right_index=True, how="left")
    
    final_gdf[list(indicators.keys())] = final_gdf[list(indicators.keys())].fillna(0)
    
    print("Performing per capita normalization...")
    if os.path.exists(population_data_csv):
        pop_df = pd.read_csv(population_data_csv)
        final_gdf = final_gdf.merge(pop_df[[lsoa_join_field, population_field_in_csv]], on=lsoa_join_field, how="left")
        
        fields_to_normalize = ["ArtVenue_Nabe", "Naptan_Nabe", "LoanValue_Nabe", "RetailAccess_Wt", "SpcSiteArea_Nabe", "Landfill_Nabe", "MajorRoad_Nabe", "NukeExp_Weighted"]
        if population_field_in_csv in final_gdf.columns:
            for field in fields_to_normalize:
                if field in final_gdf.columns:
                    per_capita_field = f"{field}_pc"
                    final_gdf[per_capita_field] = (final_gdf[field] / final_gdf[population_field_in_csv]) * 1000
        else:
            print(f"  WARNING: Population field '{population_field_in_csv}' not found. Skipping per capita.")
    else:
        print(f"  WARNING: Population data CSV not found. Skipping per capita.")

    print(f"\nSaving final output to: {final_output_gpkg}")
    final_gdf.to_file(final_output_gpkg, layer='LSOA_Master_Indicators', driver='GPKG')
    
    end_time = time.time()
    print(f"\nAll analysis complete. Total time: {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()