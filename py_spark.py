"""
PySpark-based data validator for large-scale retail datasets
Equivalent of the original pandas script with high performance and scalability
"""

import re
import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, trim, regexp_replace, when, isnan, countDistinct, lower, udf, split
from pyspark.sql.types import StringType, DoubleType

# -------- Initialize Spark Session -------- #
spark = SparkSession.builder \
    .appName("Retail Data Validator") \
    .getOrCreate()

# -------- Utility: Detect delimiter -------- #
def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.readline()
        if '|' in sample:
            return '|'
        elif '\t' in sample:
            return '\t'
        else:
            return ','

# -------- Column Normalization -------- #
def normalize_column_name(col):
    return re.sub(r'[^a-z0-9]', '', col.lower())

def standardize_columns(df):
    col_mapping = {
        'storeid': 'Store ID', 'store_id': 'Store ID', 'storeidnumber': 'Store ID',
        'category': 'Category', 'product': 'Product',
        'productid': 'Product Id', 'product_id': 'Product Id',
        'storeaddress': 'Store Address', 'storename': 'Store Name',
        'zipcode': 'Zip Code', 'zip': 'Zip Code', 'country': 'Country',
        'latitude': 'Latitude', 'longitude': 'Longitude',
        'producturl': 'Product Url', 'tmadid': 'TMAD_ID',
        'pcid': 'PC_ID', 'pcaddress': 'PC_Address',
        'tricityid': 'TRICITY_ID', 'caseysid': 'Caseys_Id',
        'uniqueproduct': 'Unique Product', 'currency': 'Currency',
        'dateextracted': 'DateExtracted', 'competitor': 'Competitor',
        'type': 'Type', 'subtypesize': 'SubType / Size', 'subtype': 'SubType / Size'
    }
    new_cols = [col_mapping.get(normalize_column_name(c), c.strip()) for c in df.columns]
    return df.toDF(*new_cols)

# -------- Core Validation Logic -------- #
def validate_data(input_path, output_path, country, ftype='RMS'):
    output_lines = []
    try:
        delimiter = detect_delimiter(input_path)
        df = spark.read.option("header", True).option("sep", delimiter).csv(input_path)
        df = standardize_columns(df)
        output_lines.append(f"✅ File loaded with delimiter '{delimiter}'\n")

        if df.count() != df.dropDuplicates().count():
            output_lines.append("⚠️ Duplicate rows found in current data. Dropping them.\n")
            df = df.dropDuplicates()

        # -------- Competitor Check -------- #
        output_lines.append("\n-------- Competitor Check --------")
        if 'Competitor' not in df.columns:
            output_lines.append("[WARN] Competitor column missing.")
        else:
            comp_values = df.select('Competitor').distinct().rdd.flatMap(lambda x: x).collect()
            if len(comp_values) != 1:
                output_lines.append(f"❌ Competitor mismatch: {comp_values}")
            else:
                output_lines.append(f"✅ Competitor: {comp_values[0]}")
        output_lines.append("-------- End Competitor Check --------\n")

        # -------- Latitude/Longitude/ZIP -------- #
        bounds = {
            "USA": (18.77, 71.5, -179.14, -66.93),
            "India": (6.75, 35.5, 68.7, 97.4),
            "Germany": (47.3, 55.1, 5.9, 15.0),
            "UK": (49.9, 58.6, -8.15, 1.8),
            "CA": (41.7, 83.1, -141.0, -52.6),
            "PH": (4.5, 21.3, 116.9, 126.6),
            "NZ": (-47.3, -34.4, 166.4, 178.6),
            "AUS": (-43.6, -10.0, 113.3, 153.6),
            "FRA": (41.3, 51.1, -5.2, 9.6)
        }.get(country, (None, None, None, None))

        if None in bounds:
            output_lines.append(f"[WARN] No bounds defined for {country}")
        else:
            df = df.withColumn("Latitude", col("Latitude").cast(DoubleType()))
            df = df.withColumn("Longitude", col("Longitude").cast(DoubleType()))
            df = df.withColumn("Zip Code", regexp_replace(col("Zip Code"), r"\\.0$", ""))

            if country == "USA":
                df = df.withColumn("Zip Code", lpad(col("Zip Code"), 5, '0'))

            invalid_lat = df.filter((col("Latitude") < bounds[0]) | (col("Latitude") > bounds[1]))
            invalid_long = df.filter((col("Longitude") < bounds[2]) | (col("Longitude") > bounds[3]))

            if invalid_lat.count() > 0:
                store_ids = invalid_lat.select("Store ID", "Latitude").dropDuplicates().collect()
                output_lines.append("Invalid Latitude entries:")
                output_lines.extend([f"Store ID: {r['Store ID']}, Latitude: {r['Latitude']}" for r in store_ids])

            if invalid_long.count() > 0:
                store_ids = invalid_long.select("Store ID", "Longitude").dropDuplicates().collect()
                output_lines.append("Invalid Longitude entries:")
                output_lines.extend([f"Store ID: {r['Store ID']}, Longitude: {r['Longitude']}" for r in store_ids])

        # ZIP validation
        zip_regex = {
            "USA": r"^\\d{5}(-\\d{4})?$",
            "India": r"^\\d{6}$",
            "Germany": r"^\\d{5}$",
            "UK": r"^[A-Z]{1,2}\\d[A-Z\\d]?\\s?\\d[A-Z]{2}$",
            "CA": r"^[A-Za-z]\\d[A-Za-z][ -]?\\d[A-Za-z]\\d$",
            "PH": r"^\\d{4}$",
            "NZ": r"^\\d{4}$",
            "AUS": r"^\\d{4}$",
            "FRA": r"^\\d{5}$"
        }.get(country, r".*")

        zip_udf = udf(lambda z: "valid" if re.match(zip_regex, z or "") else "invalid", StringType())
        df = df.withColumn("zip_status", zip_udf(col("Zip Code")))
        bad_zips = df.filter(col("zip_status") == "invalid").select("Store ID", "Zip Code").dropDuplicates().collect()
        if bad_zips:
            output_lines.append("Invalid ZIP codes:")
            output_lines.extend([f"Store ID: {r['Store ID']}, Zip: {r['Zip Code']}" for r in bad_zips])

        # -------- Null / Empty Checks -------- #
        cols_to_check = ["Category", "Product", "Price", "Product Id", "Store ID", "Store Name",
                         "Store Address", "City", "State", "Zip Code", "Country", "Latitude", "Longitude", "Product Url"]

        for colname in cols_to_check:
            if colname in df.columns:
                cleaned = df.withColumn(colname, trim(col(colname)))
                null_rows = cleaned.filter((col(colname) == "") | col(colname).isNull())
                if null_rows.count() > 0:
                    sids = null_rows.select("Store ID").dropna().distinct().rdd.flatMap(lambda x: x).collect()
                    output_lines.append(f"Empty or null '{colname}' values in Store IDs: {sids}")

        if 'Price' in df.columns:
            zero_price = df.filter(col("Price") == 0)
            if zero_price.count() > 0:
                ids = zero_price.select("Store ID").dropna().distinct().rdd.flatMap(lambda x: x).collect()
                output_lines.append(f"Zero Price found for Store IDs: {ids}")

        # -------- Special Characters -------- #
        symbol_patterns = {
            'Category': r'[^a-zA-Z0-9\s,\'\.\-&$]',
            'Product': r'[^a-zA-Z0-9\s,\'\.\-&$]',
            'Price': r'[^0-9\.]',
            'Store Name': r'[^a-zA-Z0-9\s,\/\'\.\-&,]',
            'Store Address': r'[^a-zA-Z0-9\s,\/\'\.\-&,]',
            'Latitude': r'[^0-9\.\-]',
            'Longitude': r'[^0-9\.\-]',
            'Store ID': r'[^a-zA-Z0-9,\-]',
            'Product Id': r'[^a-zA-Z0-9,\-]',
            'City': r'[^a-zA-Z0-9,\-]',
            'State': r'[^a-zA-Z0-9,\-]',
        }
        for colname, pattern in symbol_patterns.items():
            if colname in df.columns:
                bad = df.filter(col(colname).rlike(pattern)).select("Store ID").dropna().distinct()
                if bad.count() > 0:
                    store_ids = bad.rdd.flatMap(lambda x: x).collect()
                    output_lines.append(f"Special characters in '{colname}' for Store IDs: {store_ids}")

        # -------- Duplicate Product Check -------- #
        prod_cols = ['Store ID', 'Category', 'Product', 'Type', 'SubType / Size']
        if all(c in df.columns for c in prod_cols):
            dup = df.groupBy(prod_cols).count().filter("count > 1")
            if dup.count() > 0:
                dups = dup.select("Store ID").dropDuplicates().rdd.flatMap(lambda x: x).collect()
                output_lines.append(f"🟡 Duplicate product entries in Store IDs: {dups}")
            else:
                output_lines.append("✅ No duplicate product entries found.")

        # -------- Scrape Dates -------- #
        if 'DateExtracted' in df.columns:
            date_parts = df.select(split(col("DateExtracted"), " ").getItem(0).alias("date"))
            distinct_dates = date_parts.distinct().rdd.flatMap(lambda x: x).collect()
            output_lines.append("Scraping Dates: " + ", ".join(distinct_dates))

        output_lines.append("\n===== VALIDATION COMPLETE =====")

    except Exception as e:
        output_lines.append(f"[FATAL ERROR] Validation failed: {str(e)}")
        traceback.print_exc()

    # -------- Write output -------- #
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            for line in output_lines:
                f.write(str(line) + '\n')
        print(f"✅ Validation log saved at: {output_path}")
    except Exception as fe:
        print(f"[ERROR] Failed to write output file: {fe}")


# Example usage (update with actual paths when running):
# validate_data("/path/to/current_file.csv", "output_summary.txt", country="USA")
