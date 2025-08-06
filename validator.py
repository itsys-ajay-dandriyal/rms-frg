import pandas as pd
import os
import traceback
import re

def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.readline()
        if '|' in sample:
            return '|'
        elif '\t' in sample:
            return '\t'
        else:
            return ','

def normalize_column_name(col):
    col = col.lower()
    col = re.sub(r'[^a-z0-9]', '', col)
    return col

def standardize_columns(df):
    col_mapping = {
        'storeid': 'Store ID',
        'store_id': 'Store ID',
        'storeidnumber': 'Store ID',
        'category': 'Category',
        'product': 'Product',
        'productid': 'Product Id',
        'product_id': 'Product Id',
        'storeaddress': 'Store Address',
        'storename': 'Store Name',
        'zipcode': 'Zip Code',
        'zip': 'Zip Code',
        'country': 'Country',
        'latitude': 'Latitude',
        'longitude': 'Longitude',
        'producturl': 'Product Url',
        'tmadid': 'TMAD_ID',
        'pcid': 'PC_ID',
        'pcaddress': 'PC_Address',
        'tricityid': 'TRICITY_ID',
        'caseysid': 'Caseys_Id',
        'uniqueproduct': 'Unique Product',
        'currency': 'Currency',
        'dateextracted': 'DateExtracted',
        'competitor': 'Competitor',
        'type': 'Type',
        'subtypesize': 'SubType / Size',
        'subtype' : 'SubType / Size'
        # Add more mappings if needed
    }

    df.columns = [col_mapping.get(normalize_column_name(col), col.strip()) for col in df.columns]
    return df

#--file type & distinct count & symbol check--
def type_of_file(c_data, p_data, output_lines, ftype):
    if ftype == 'RMS':
        allowed_symbols = {
            'Unique Product': r'[^a-zA-Z0-9\s,\'\.\-&$]',
            'Currency': r'[^a-zA-Z]',
        }
    elif ftype == 'TRICITY':
        allowed_symbols = {
            'TRICITY_ID': r'[^a-zA-Z0-9,\-]'
        }
        output_lines.append(f"Distinct Count(Tricity id): Current: {c_data['TRICITY_ID'].nunique()}, Previous: {p_data['TRICITY_ID'].nunique()}") 
    elif ftype == 'TMAD':
        allowed_symbols = {
            'TMAD_ID': r'[^a-zA-Z0-9,\-]',
            'Currency': r'[^a-zA-Z]'
        }
        output_lines.append(f"Distinct Count(TMAD id): Current: {c_data['TMAD_ID'].nunique()}, Previous: {p_data['TMAD_ID'].nunique()}")
    elif ftype == 'POLLO_CAMPERO':
        allowed_symbols = {
            'PC_ID': r'[^a-zA-Z0-9,\-]',
            'PC_Address': r'[^a-zA-Z0-9\s,\/\'\.\-&,]'
        }
        output_lines.append(f"Distinct Count(POLLO id): Current: {c_data['PC_ID'].nunique()}, Previous: {p_data['PC_ID'].nunique()}")
    elif ftype == 'Caseys':
        allowed_symbols = {
            'Caseys_Id' : r'[^a-zA-Z0-9,\-]'
        }
        output_lines.append(f"Distinct Count(Caseys id): Current: {c_data['Caseys_Id'].nunique()}, Previous: {p_data['Caseys_Id'].nunique()}")


    for col, pattern in allowed_symbols.items():
        if col not in c_data.columns:
            output_lines.append(f"[WARN] Column '{col}' not found for symbol check.")
            continue

        try:
            c_data[col] = c_data[col].astype(str).str.strip().replace('nan', '')
            bad_rows = c_data[c_data[col].str.contains(pattern, regex=True, na=False)]

            if not bad_rows.empty:
                store_ids = bad_rows['Store ID'].drop_duplicates().tolist()
                output_lines.append(f"Special character found in '{col}' for Store IDs: {store_ids}")
        except Exception as e:
            output_lines.append(f"[ERROR] Issue processing column '{col}' during symbol check: {str(e)}")


#------Category not in current data------
def cat_not_in(c_data, p_data, output_lines):
    try:
        # Ensure required columns are present
        required_columns = {'Store ID', 'Category'}
        if not required_columns.issubset(c_data.columns) or not required_columns.issubset(p_data.columns):
            output_lines.append("Missing required columns in input data.")
            return

        # Group categories by store for both datasets
        try:
            c_grouped = c_data.groupby('Store ID')['Category'].apply(set)
            p_grouped = p_data.groupby('Store ID')['Category'].apply(set)
        except Exception as e:
            output_lines.append(f"Error while grouping data: {e}")
            return

        # Find common stores
        common_ids = c_grouped.index.intersection(p_grouped.index)

        # Check for category differences
        results = []
        for store_id in common_ids:
            try:
                diff = p_grouped[store_id] - c_grouped[store_id]
                if diff:
                    results.append(f"{store_id} : {list(diff)}")
            except Exception as e:
                results.append(f"{store_id} : Error comparing categories - {e}")

        if results:
            output_lines.append("Category not in current (Store ID, Category):\n" + "\n".join(results))
        else:
            output_lines.append("No missing categories found in current data.")

    except Exception as e:
        output_lines.append(f"Unexpected error: {e}")


#-------Check For Valid Lat, Long--------
def lat_long_zip(country, c_data, output_lines):
    try:
        bounds = {
            "USA": (18.77, 71.5, -179.14, -66.93),
            "India": (6.75, 35.5, 68.7, 97.4),
            "Germany": (47.3, 55.1, 5.9, 15.0),
            "UK": (49.9, 58.6, -8.15, 1.8),
            "CA": (41.7, 83.1, -141.0, -52.6),
            "PH": (4.5, 21.3, 116.9, 126.6),
            'NZ':(-47.3, 166.4, -34.4, 178.6),
            "AUS": (-43.6, -10.0, 113.3, 153.6),
            "FRA": (41.3, 51.1, -5.2, 9.6)
        }.get(country, (None, None, None, None))

        if None in bounds:
            output_lines.append(f"[WARN] No lat/long bounds defined for country: {country}")
            return

        c_data['Latitude'] = pd.to_numeric(c_data['Latitude'], errors='coerce')
        c_data['Longitude'] = pd.to_numeric(c_data['Longitude'], errors='coerce')

        invalid_lat = c_data[(c_data['Latitude'] < bounds[0]) | (c_data['Latitude'] > bounds[1])]
        if not invalid_lat.empty:
            invalid_lat = invalid_lat[['Store ID', 'Latitude']].drop_duplicates()
            output_lines.append("Invalid Latitude entries:")
            output_lines.extend(invalid_lat[['Store ID', 'Latitude']].astype(str).apply(
                lambda x: f"Store ID: {x['Store ID']}, Latitude: {x['Latitude']}", axis=1).tolist())

        invalid_long = c_data[(c_data['Longitude'] < bounds[2]) | (c_data['Longitude'] > bounds[3])]
        if not invalid_long.empty:
            invalid_long = invalid_long[['Store ID', 'Longitude']].drop_duplicates()
            output_lines.append("Invalid Longitude entries:")
            output_lines.extend(invalid_long[['Store ID', 'Longitude']].astype(str).apply(
                lambda x: f"Store ID: {x['Store ID']}, Longitude: {x['Longitude']}", axis=1).tolist())

        # Ensure ZIP column is string and cleaned
        if 'Zip Code' in c_data.columns:
            c_data['Zip Code'] = c_data['Zip Code'].astype(str).str.strip()

        # Remove decimal ".0" if ZIPs came in as float strings
        c_data['Zip Code'] = c_data['Zip Code'].str.replace(r'\.0$', '', regex=True)

        # Pad ZIPs with leading 0s (especially for US ZIPs like 02108)
        if country == "USA":
            c_data['Zip Code'] = c_data['Zip Code'].str.zfill(5)

        zip_regex = {
            "USA": r"^\d{5}(-\d{4})?$",
            "India": r"^\d{6}$",
            "Germany": r"^\d{5}$",
            "UK": r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$",
            "CA": r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$",
            "PH": r"^\d{4}$",
            "NZ": r"^\d{4}$",
            "AUS": r"^\d{4}$",
            "FRA": r"^\d{5}$"
        }.get(country, r".*")

        # Filter invalid zip codes
        invalid_zip = c_data[~c_data['Zip Code'].str.match(zip_regex, na=False)]

        # Check if there are any invalid zip codes
        if not invalid_zip.empty:
            output_lines.append("Invalid ZIP codes found:")
            # Create a list of Store ID and Zip Code combinations
            unique_invalid_zips = invalid_zip[['Store ID', 'Zip Code']].drop_duplicates()
            # Format the output and convert it into a list of strings
            formatted_invalid_zips = unique_invalid_zips.apply(
                lambda x: f"Store ID: {x['Store ID']}, Zip: {x['Zip Code']}", axis=1).tolist()
            output_lines.extend(formatted_invalid_zips)


    except Exception as e:
        output_lines.append(f"[ERROR] lat_long_zip() failed: {e}")


#-------Check empty & Special Character-----
def check_null_values(c_data, output_lines, ftype):
    cols_to_check = ['Category', 'Product', 'Price', 'Product Id', 'Store ID', 'Store Name',
                     'Store Address', 'City', 'State', 'Zip Code', 'Country', 'Latitude', 'Longitude', 'Product Url']
    
    if ftype == 'RMS':
        output_lines.append("File type RMS")
        cols_to_check.extend(['Unique Product', 'Currency'])
    elif ftype == 'TMAD':
        output_lines.append("File type TMAD")
        cols_to_check.extend(['TMAD_ID', 'Currency'])
    elif ftype == 'POLLO_CAMPERO':
        output_lines.append("File type POLLO CAMPERO")
        cols_to_check.extend(['PC_ID', 'PC_Address'])
    elif ftype == 'TRICITY':
        output_lines.append("File type TRICITY")
        cols_to_check.append('TRICITY_ID') 
    elif ftype == 'Caseys':
        output_lines.append("File type Caseys")
        cols_to_check.append('Caseys_Id')           

    missing_cols = [col for col in cols_to_check if col not in c_data.columns]
    if missing_cols:
        for col in missing_cols:
            output_lines.append(f"[WARN] Column '{col}' not found in dataset.")

    for col in cols_to_check:
        if col not in c_data.columns:
            continue

        try:
            c_data[col] = c_data[col].astype(str).str.strip().replace('nan', '')
            empty_mask = c_data[col].eq('')
            if empty_mask.any():
                store_ids = c_data.loc[empty_mask, 'Store ID'].dropna().unique()
                output_lines.append(f"Empty or null '{col}' values in Store IDs: {list(store_ids)}")
        except Exception as e:
            output_lines.append(f"[ERROR] Issue processing column '{col}' during null check: {str(e)}")

    try:
        if 'Price' in c_data.columns:
            c_data['Price'] = pd.to_numeric(c_data['Price'], errors='coerce')
            zero_mask = c_data['Price'] == 0
            
            if zero_mask.any():
                ids = c_data.loc[zero_mask, 'Store ID'].dropna().unique()
                output_lines.append(f"Zero Price found for Store IDs: {list(ids)}")
    except Exception as e:
        output_lines.append(f"[ERROR] Price null check failed: {e}")        

# --------Check for unwanted Symbol-------
def check_symbol_violations(c_data, output_lines):
    # Define regex patterns to find **disallowed** characters
    allowed_symbols = {
        'Category': r'[^a-zA-Z0-9\s,\'\.\-&$]',
        'Product': r'[^a-zA-Z0-9\s,\'\.\-&$]',
        'Price': r'[^0-9.]',
        'Store Name': r'[^a-zA-Z0-9\s,\/\'\.\-&,]',
        'Store Address': r'[^a-zA-Z0-9\s,\/\'\.\-&,]',
        'Latitude': r'[^0-9\.\-]',
        'Longitude': r'[^0-9\.\-]',
        'Store ID': r'[^a-zA-Z0-9,\-]',
        'Product Id': r'[^a-zA-Z0-9,\-]'
    }

    for col, pattern in allowed_symbols.items():
        if col not in c_data.columns:
            output_lines.append(f"[WARN] Column '{col}' not found for symbol check.")
            continue

        try:
            c_data[col] = c_data[col].astype(str).str.strip().replace('nan', '')
            bad_rows = c_data[c_data[col].str.contains(pattern, regex=True, na=False)]

            if not bad_rows.empty:
                store_ids = bad_rows['Store ID'].drop_duplicates().tolist()
                output_lines.append(f"Special character found in '{col}' for Store IDs: {store_ids}")
        except Exception as e:
            output_lines.append(f"[ERROR] Issue processing column '{col}' during symbol check: {str(e)}")
        

#----Store Id not in Current----------
def not_in_prev(c_data, p_data, output_lines):
    unique_c_ids = c_data['Store ID'].unique()
    unique_p_ids = p_data['Store ID'].unique()
    
    store_ids_not_in_current = [store_id for store_id in unique_p_ids if store_id not in unique_c_ids]

    if store_ids_not_in_current:
        output_lines.append(f"Store IDs Not in Current Data: {store_ids_not_in_current}")


# ------- Duplicate Product Check -------
def check_duplicate_products(c_data, output_lines):
    required_cols = ['Store ID', 'Category', 'Product', 'Type', 'SubType / Size']
    if all(col in c_data.columns for col in required_cols):
        dup_mask = c_data.duplicated(subset=required_cols)
        if dup_mask.any():
            dup_ids = c_data.loc[dup_mask, 'Store ID'].dropna().unique()
            output_lines.append(f"🟡 Duplicate product entries in Store IDs: {list(dup_ids)}")
        else:
            output_lines.append("✅ No duplicate product entries found.")
    else:
        missing = [col for col in required_cols if col not in c_data.columns]
        output_lines.append(f"[WARN] Missing columns for product duplicate check: {missing}")


def run_validation(ip1, ip2, country, output_path, ftype):
    output_lines = []
    try:
        output_lines.append("============STARTING VALIDATION ===========")

        delimiter = detect_delimiter(ip1)

        c_data = pd.read_csv(ip1, sep=delimiter, dtype=str)
        c_data = standardize_columns(c_data)

        p_data = pd.read_csv(ip2, sep=delimiter, dtype=str)
        p_data = standardize_columns(p_data)

        output_lines.append(f"✅ Files loaded successfully using delimiter: '{delimiter}'")

        if c_data.duplicated().any():
            dup_rows = c_data[c_data.duplicated()]
            output_lines.append(f"⚠️ Duplicate rows found in current data: {len(dup_rows)}")
            output_lines.append(dup_rows.to_string(index=False))
            c_data = c_data.drop_duplicates()

        # Competitor Check
        output_lines.append("\n-------- Start: Competitor Check --------")
        if 'Competitor' not in c_data.columns or 'Competitor' not in p_data.columns:
            output_lines.append("[WARN] Competitor column missing in one or both files.")
        else:
            c_comp = c_data['Competitor'].dropna().unique()
            p_comp = p_data['Competitor'].dropna().unique()
            if len(c_comp) != 1 or len(p_comp) != 1 or c_comp[0] != p_comp[0]:
                output_lines.append("❌ Competitor mismatch:")
                output_lines.append(f"Current: {list(c_comp)} | Previous: {list(p_comp)}")
            else:
                output_lines.append(f"✅ Competitor: {c_comp[0]}")
        output_lines.append("--------- End: Competitor Check ---------\n")

        # Data Counts
        output_lines.append(f"Total Count->   Current: {c_data.shape},  Previous: {p_data.shape}")
        output_lines.append(f"Distinct(Store IDs)->   Current: {c_data['Store ID'].nunique()},  Previous: {p_data['Store ID'].nunique()}")

        try:
            total_rows = c_data.shape[0] + p_data.shape[0]
            if total_rows == 0:
                output_lines.append("⚠️ Both datasets are empty.")
            else:
                diff_percent = ((c_data.shape[0] - p_data.shape[0]) / total_rows) * 100
                if diff_percent > 0:
                    output_lines.append(f"📈 Current data increased by {round(diff_percent, 2)}%")
                else:
                    output_lines.append(f"📉 Current data decreased by {abs(round(diff_percent, 2))}%")
        except Exception as e:
            output_lines.append(f"[ERROR] Calculating data difference: {e}")

        if ftype == 'FRG':
            output_lines.append("file type is FRG")
        else:
            output_lines.append("\n------ Start: File Type Check ---------")
            type_of_file(c_data, p_data, output_lines, ftype)
            output_lines.append("------ End: File Type Check -----------\n")

        if c_data['Country'].nunique() > 1:
            output_lines.append("❌ Country more than one")

        # Sectioned function calls
        output_lines.append("\n------- Start: Latitude/Longitude/ZIP Check ------")
        lat_long_zip(country, c_data, output_lines)
        output_lines.append("----- End: Latitude/Longitude/ZIP Check -----\n")

        output_lines.append("\n------ Start: Null Value Check ------")
        check_null_values(c_data, output_lines, ftype)
        output_lines.append("------ End: Null Value Check ------\n")

        output_lines.append("\n----- Start: Symbol Violation Check -----")
        check_symbol_violations(c_data, output_lines)
        output_lines.append("----- End: Symbol Violation Check ------\n")

        output_lines.append("\n------ Start: Duplicate Product Check ----")
        check_duplicate_products(c_data, output_lines)
        output_lines.append("----- End: Duplicate Product Check -----\n")

        output_lines.append("\n----- Start: Store ID Not in Current -----")
        not_in_prev(c_data, p_data, output_lines)
        output_lines.append("----- End: Store ID Not in Current -----\n")

        output_lines.append("\n--- Start: Category Not in Current ---")
        cat_not_in(c_data, p_data, output_lines)
        output_lines.append("--- End: Category Not in Current ---\n")

        # DateExtracted check
        unique_dates = c_data['DateExtracted'].unique()
        unique_date = [udate.split()[0] for udate in unique_dates]
        unique_date = list(set(unique_date))
        output_lines.append(f"Dates of Scraping: {', '.join(unique_date)}")

        output_lines.append("===== VALIDATION COMPLETE =====")

    except Exception as main_err:
        traceback.print_exc()
        output_lines.append(f"[FATAL ERROR] Validation failed: {main_err}")

    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(str(line) + '\n')
    except Exception as file_err:
        output_lines.append(f"[ERROR] Could not write log file: {file_err}")

    return os.path.abspath(output_path)
# ip1 = r"C:\Users\ITSYS-PC13\Desktop\panda_validator\2025_08_Wendys.txt"
# ip2 = r"C:\Users\ITSYS-PC13\Desktop\panda_validator\2025_07_Wendys.txt"
# run_validation(ip1, ip2, country="USA", output_path="output_summary.txt", ftype='RMS')
