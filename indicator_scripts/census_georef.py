import arcpy
import os
from pathlib import Path

# --- User configuration ---
lsoa_shapefile = r"M:\Dissertation\LSOA_boundaries\LSOA_2021_EW_BSC_V4.shp"  # Path to LSOA shapefile
csv_dir = r"M:\Dissertation\census\lsoa_csv_inputs"  # Folder with cleaned CSVs
output_parent_dir = r"M:\Dissertation\census\georeferenced"  # Parent directory for the GDB

# Define the full path for your output File Geodatabase
output_gdb_name = "CensusJoinedData.gdb"
output_gdb_path = os.path.join(output_parent_dir, output_gdb_name)

# --- Create output parent directory if needed ---
Path(output_parent_dir).mkdir(parents=True, exist_ok=True)

# --- Create the File Geodatabase if it does not exist ---
print(f"Checking for/creating GDB: {output_gdb_path}")
if not arcpy.Exists(output_gdb_path):
    arcpy.management.CreateFileGDB(output_parent_dir, output_gdb_name)
    print(f"File Geodatabase '{output_gdb_name}' created at {output_parent_dir}")
else:
    print(f"File Geodatabase '{output_gdb_name}' already exists.")

# --- Set workspace and environment ---
arcpy.env.overwriteOutput = True
# Set the workspace to your GDB for easier management of outputs within it
arcpy.env.workspace = output_gdb_path 

# --- Loop through CSV files ---
for csv_file in Path(csv_dir).glob("*.csv"):
    csv_name = csv_file.stem
    temp_table_view = f"{csv_name}_view" # Use a view name

    # Define the output path for the new Feature Class within the GDB
    # Feature class names within a GDB do not need a file extension (.shp)
    output_feature_class_name = f"lsoa_{csv_name}"
    output_path = os.path.join(output_gdb_path, output_feature_class_name)

    try:
        print(f"\n--- Processing {csv_name} ---")

        # 1. Convert CSV to an in-memory table view
        print(f"Creating table view for {csv_file.name}...")
        arcpy.management.MakeTableView(str(csv_file), temp_table_view)
        print("Table view created.")

        # 2. Create feature layer from LSOA shapefile
        lsoa_layer = "lsoa_lyr"
        print(f"Creating feature layer from {lsoa_shapefile}...")
        arcpy.management.MakeFeatureLayer(lsoa_shapefile, lsoa_layer)
        print("Feature layer created.")

        # 3. Join table to LSOA layer
        # The 'mnemonic' field in your CSV must correspond to 'LSOA21CD' in your LSOA shapefile.
        print(f"Adding join from {temp_table_view} to {lsoa_layer} on LSOA21CD / mnemonic...")
        arcpy.management.AddJoin(lsoa_layer, "LSOA21CD", temp_table_view, "mnemonic", "KEEP_COMMON")
        print("Join added.")

        # --- Crucial Step: Rename fields on the joined layer BEFORE exporting ---
        # This will prevent the "temp_table_view_" prefix in the output.
        print("Renaming joined fields...")
        field_list = arcpy.ListFields(lsoa_layer)
        
        # Get the fields from the original CSV to compare against for renaming
        # We need to explicitly get the field names from the table view, as ListFields on
        # a joined layer might show them already prefixed.
        # So we'll get field names directly from the original CSV (or the table view if preferred).
        
        # Method 1: Get field names directly from the temporary table view (after MakeTableView)
        csv_original_fields = [f.name for f in arcpy.ListFields(temp_table_view)]

        # Method 2 (less reliable with MakeTableView, but good for TableToTable):
        # reader = csv.reader(open(csv_file, 'r'))
        # csv_original_fields = next(reader) # Get header row

        for field in field_list:
            # Check if the field name starts with the CSV table view name prefix
            # and is not one of the original LSOA fields (like 'LSOA21CD', 'Shape_Area', etc.)
            
            # ArcGIS Join can prepend 'temp_table_view_'. Let's check for both '.' and '_'
            # patterns if the field is not a default LSOA field.

            # We assume LSOA21CD is always from the LSOA layer and doesn't need renaming.
            # 'mnemonic' from the CSV is the join field, and we likely don't want to keep it as a duplicate in the output
            # or it might be renamed by ArcGIS if there's an LSOA field with same name.
            
            # The pattern for joined field names is typically "JoinTableName.FieldName" or "JoinTableName_FieldName"
            # So, we'll try to extract "FieldName" part and see if it matches our original CSV headers.
            
            original_field_from_csv = None

            # Pattern 1: 'TableViewName.FieldName'
            parts_dot = field.name.split('.')
            if len(parts_dot) > 1 and parts_dot[0] == temp_table_view:
                potential_original_name = parts_dot[-1]
                if potential_original_name in csv_original_fields:
                    original_field_from_csv = potential_original_name
            
            # Pattern 2: 'TableName_FieldName' (more common for shapefile export, but can appear in layer too)
            # This is trickier if original field names contain underscores.
            # A more robust way might involve tracking original field objects.
            # For simplicity, we assume original CSV field names don't clash much with system-added prefixes.
            elif field.name.startswith(f"{csv_name}_"): # Check if it starts with the CSV's base name
                # Try to remove the prefix based on common ArcGIS behavior
                potential_original_name = field.name[len(f"{csv_name}_"):].strip()
                if potential_original_name in csv_original_fields:
                    original_field_from_csv = potential_original_name

            # Now, if we found a match and it's not the 'mnemonic' field (which is redundant after join)
            if original_field_from_csv and original_field_from_csv != "mnemonic":
                if field.name != original_field_from_csv: # Only alter if actual name is different
                    print(f"  Renaming '{field.name}' to '{original_field_from_csv}'")
                    arcpy.management.AlterField(lsoa_layer, field.name, original_field_from_csv, original_field_from_csv)
            elif original_field_from_csv == "mnemonic":
                # If 'mnemonic' is found, you might want to remove it as 'LSOA21CD' covers the ID
                # Removing fields with AlterField is not direct; you'd need to rebuild the FC or use FieldMappings in CopyFeatures (if supported, not for shapefile)
                # For now, we'll let it be dropped by default if not explicitly renamed, or just ignore.
                # If you want to explicitly drop it, you'd need to create a FieldMappings object for CopyFeatures.
                pass # Do nothing, it will be kept with its joined name or omitted if not copied explicitly.


        # 4. Export to new Feature Class within the GDB
        print(f"Exporting joined data to Feature Class: {output_path}...")
        arcpy.management.CopyFeatures(lsoa_layer, output_path)
        print(f"✓ Joined and exported: {csv_name}")

    except arcpy.ExecuteError:
        messages = arcpy.GetMessages(2)
        print(f"⚠️ ArcGIS Error processing {csv_name}:\n{messages}")
    except Exception as e:
        print(f"⚠️ Python Error processing {csv_name}: {e}")
    finally:
        # 5. Clean up
        print("Cleaning up temporary layers and views...")
        if arcpy.Exists(lsoa_layer):
            arcpy.management.RemoveJoin(lsoa_layer)
            arcpy.management.Delete(lsoa_layer) # Delete the temporary feature layer
        if arcpy.Exists(temp_table_view):
            arcpy.management.Delete(temp_table_view) # Delete the temporary table view
        print(f"--- Finished processing {csv_name} ---")

print("\nBatch processing complete!")
