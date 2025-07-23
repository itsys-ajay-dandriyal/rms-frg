import logging
import os
from datetime import datetime
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql.functions import col, countDistinct, expr

#---file_type_check---
def detect_delimiter(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.readline()
        if '|' in sample:
            return '|'
        elif '\t' in sample:
            return '\t'
        else:
            return ','

def run_validation(current_path, previous_path, country):
    
    delimiter = detect_delimiter(current_path)

    spark = SparkSession.builder.appName("DataValidation").getOrCreate()
    df_current = spark.read.option("header", True).option("delimiter", delimiter).csv(current_path)
    df_previous = spark.read.option("header", True).option("delimiter", delimiter).csv(previous_path)

    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/validation_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w', encoding='utf-8')

    def log(msg): logging.info(msg)

    log("----------------Validation Report--------------")
    
    comp = df_current.select('Competitor').distinct()

    # Fix: Collect only distinct Competitor values and log them as actual strings
    competitor_values = [str(row['Competitor']) for row in comp.collect()]
    log(f"Competitor : {', '.join(competitor_values)}")

    row_diff = abs(df_current.count() - df_previous.count()) / max(df_previous.count(), 1)
    if row_diff > 0.02:
        log(f"Row count differs by more than 2%: {row_diff:.2%}")

    compet = "Competitor"
    if compet not in df_current.columns:
        log(f" Column '{compet}' not found.")
    else:
        null_ids = df_current.filter(col(compet).isNull()).count()
        if null_ids > 0:
            log(f" {null_ids} rows with null {compet}")


        log(f"Total Count->  Current: {df_current.count(), len(df_current.columns)} \
            Previous: {df_previous.count(), len(df_previous.columns)}")
        
        log(f"Distinct Count -> Current: {df_current.select('Store ID').distinct().count()} \
            Previous: {df_previous.select('Store ID').distinct().count()}")

        
        current_ids = df_current.select('Store ID').distinct()
        previous_ids = df_previous.select('Store ID').distinct()

        missing_ids = previous_ids.subtract(current_ids).count()
        if missing_ids > 0:
            log(f" {missing_ids} {'Store ID'}s from previous missing in current")

    #-----Duplicat_Lat_Long-----
    geo_cols = ("Latitude", "Longitude")
    if all(c in df_current.columns for c in ('Store ID', *geo_cols)):
        duplicates = df_current.groupBy('Store ID', *geo_cols).count().filter("count > 1").count()
        if duplicates > 0:
            log(f" {duplicates} duplicate ({'Store ID'}, Latitude, Longitude) combinations")
    else:
        log(" Required geo columns not found for duplication check.")

    
    from pyspark.sql.functions import countDistinct

    mapping_checks = [
        ('Store ID', "Store Address"),
        ('Store ID', "City"),
        ('Store ID', "State")
    ]

    for k1, k2 in mapping_checks:
        if k1 in df_current.columns and k2 in df_current.columns:
            inconsistent_df = df_current.groupBy(k1).agg(countDistinct(k2).alias("unique_vals")).filter("unique_vals > 1")
            inconsistent = inconsistent_df.count()
            
            if inconsistent > 0:
                log(f"❌ Inconsistent mapping: {k1} → {k2} has {inconsistent} inconsistencies")
                
                # Log specific IDs
                bad_ids = [row[k1] for row in inconsistent_df.select(k1).collect()]
                log(f"    Problematic {k1}s: {bad_ids}")
        else:
            log(f"⚠️ Missing column(s): {k1}, {k2}")

    
        
    zip_col = "Zip Code"
    zip_regex = {
        "USA": r"^\d{5}(-\d{4})?$",
        "India": r"^\d{6}$",
        "Germany": r"^\d{5}$",
        "UK": r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$",
        "CA": r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$"
    }.get(country, r".*")

    if zip_col in df_current.columns and "Store ID" in df_current.columns:
        invalid_df = df_current.filter(~trim(col(zip_col).cast("string")).rlike(zip_regex))
        invalid_count = invalid_df.count()
        if invalid_count > 0:
            log(f"❌ {invalid_count} invalid zip codes for {country}")
            bad_ids = [row["Store ID"] for row in invalid_df.select("Store ID").distinct().collect()]
            log(f"    Store IDs with invalid zips: {bad_ids}")
    else:
        log(f"⚠️ Required column(s) '{zip_col}' or 'Store ID' not found.")

    log("\n Geo Bounds")
    bounds = {
        "USA": (24.5, 49.5, -125, -66),
        "India": (6, 37, 68, 97),
        "Germany": (47, 55, 5, 15),
        "UK": (49, 60, -9, 2),
        "CA": (41.7, 83.1, -141.0, -52.6)
    }.get(country, (None, None, None, None))
    lat_col = "Latitude"
    lon_col = "Longitude"
    if lat_col in df_current.columns and lon_col in df_current.columns:
        df_current = df_current.withColumn(lat_col, expr(f"try_cast({lat_col} as double)"))
        df_current = df_current.withColumn(lon_col, expr(f"try_cast({lon_col} as double)"))

        lat_invalid = df_current.filter((col(lat_col) < bounds[0]) | (col(lat_col) > bounds[1])).count()
        lon_invalid = df_current.filter((col(lon_col) < bounds[2]) | (col(lon_col) > bounds[3])).count()
        if lat_invalid > 0 or lon_invalid > 0:
            log(f" {lat_invalid} invalid latitudes or {lon_invalid} invalid longitudes")
    else:
        log(" Latitude or Longitude column missing.")

   
    price_col = "Price"
    if price_col in df_current.columns:
        price_stats = df_current.withColumn("p", col(price_col).cast("float")).select("p").summary("min", "max").collect()
        if price_stats:
            min_val = price_stats[0]['p'] if price_stats[0]['p'] is not None else 'N/A'
            max_val = price_stats[1]['p'] if price_stats[1]['p'] is not None else 'N/A'
            log(f"ℹ Price Summary: min={min_val}, max={max_val}")
    else:
        log(" Price column not found.")

    
    currency_col = "Price"
    currency_symbols = {
        "USA": "$",
        "India": "₹",
        "Germany": "€",
        "UK": "£",
        "CA": "$"
    }
    currency_symbol = currency_symbols.get(country)
    if currency_symbol and currency_col in df_current.columns:
        missing_currency = df_current.filter(~col(currency_col).contains(currency_symbol)).count()
        if missing_currency > 0:
            log(f" {missing_currency} rows missing currency symbol '{currency_symbol}'")
    else:
        log(" Currency check skipped (symbol or column missing).")

    return os.path.abspath(log_file)
