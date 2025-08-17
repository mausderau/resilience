import sys
import os
from tqdm import tqdm

# This script does not need the user site-packages hack if only using arcpy
import arcpy

# --- CONFIGURATION ---
# EDIT THESE PATHS TO MATCH YOUR SYSTEM
# The folder containing the rasters from the previous step
INPUT_RASTER_FOLDER = r"M:\Dissertation\xRI_independent_tools\tool_outputs\6_solar_radiation" 
# The full path and name for the final output mosaic
OUTPUT_MOSAIC_PATH = r"M:\Dissertation\xRI_independent_tools\tool_outputs\7_mosaics\solar_radiance_mosaic.tif"
# The prefix of the files you want to mosaic
RASTER_PREFIX = "solar_"
# ---------------------

def main():
    """Main execution function."""
    print("Starting Step 7: Mosaic Rasters...")
    arcpy.env.workspace = INPUT_RASTER_FOLDER
    arcpy.env.overwriteOutput = True

    try:
        arcpy.CheckOutExtension("Spatial")

        raster_list = arcpy.ListRasters(f"{RASTER_PREFIX}*.tif")
        if not raster_list:
            print(f"ERROR: No rasters found in '{INPUT_RASTER_FOLDER}' with prefix '{RASTER_PREFIX}'.")
            return
        
        print(f"Found {len(raster_list)} rasters to mosaic.")

        # Using Mosaic To New Raster is robust
        arcpy.management.MosaicToNewRaster(
            input_rasters=raster_list,
            output_location=os.path.dirname(OUTPUT_MOSAIC_PATH),
            raster_dataset_name_with_extension=os.path.basename(OUTPUT_MOSAIC_PATH),
            pixel_type="32_BIT_FLOAT",  # Use floating point for height values
            number_of_bands=1,
            mosaic_method="MEAN"  # How to handle overlapping areas
        )
        if arcpy.Exists(OUTPUT_MOSAIC_PATH):
            print(f"\nSUCCESS: Mosaic complete. Output saved to: {OUTPUT_MOSAIC_PATH}")
        else:
            print(f"\nERROR: Mosaic process completed but the output file was not found.")
   

    except arcpy.ExecuteError:
        print(f"\nARCPY ERROR:")
        print(arcpy.GetMessages(2))
    except Exception as e:
        print(f"\nPYTHON ERROR: {e}")
    finally:
        arcpy.CheckInExtension("Spatial")

if __name__ == "__main__":
    main()