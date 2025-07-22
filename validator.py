import logging
import os
from datetime import datetime
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, expr

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
    # Detect delimiter
    delimiter = detect_delimiter(current_path)

    spark = SparkSession.builder.appName("DataValidation").getOrCreate()
    df_current = spark.read.option("header", True).option("delimiter", delimiter).csv(current_path)
    df_previous = spark.read.option("header", True).option("delimiter", delimiter).csv(previous_path)

    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/validation_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    logging.basicConfig(filename=log_file, level=logging.INFO, filemode='w')

    def log(msg): logging.info(msg)

    log("🔍 Row Count Validation")
    
    comp = df_current.select('Competitor').distinct()

    # Fix: Collect only distinct Competitor values and log them
    competitor_values = [row['Competitor'] for row in comp.collect()]
    log(f"Competitor is : {competitor_values}")

    row_diff = abs(df_current.count() - df_previous.count()) / max(df_previous.count(), 1)
    if row_diff > 0.02:
        log(f"❌ Row count differs by more than 2%: {row_diff:.2%}")

    log("\n🔍 Store/Competitor ID Checks")
    id_col = "Competitor"
    if id_col not in df_current.columns:
        log(f"⚠️ Column '{id_col}' not found.")
    else:
        null_ids = df_current.filter(col(id_col).isNull()).count()
        if null_ids > 0:
            log(f"❌ {null_ids} rows with null {id_col}")

        current_ids = df_current.select(id_col).distinct()
        previous_ids = df_previous.select(id_col).distinct()
        missing_ids = previous_ids.subtract(current_ids).count()
        if missing_ids > 0:
            log(f"❌ {missing_ids} {id_col}s from previous missing in current")

    log("\n🔍 Duplicate ID + Geo")
    geo_cols = ("Latitude", "Longitude")
    if all(c in df_current.columns for c in (id_col, *geo_cols)):
        duplicates = df_current.groupBy(id_col, *geo_cols).count().filter("count > 1").count()
        if duplicates > 0:
            log(f"❌ {duplicates} duplicate ({id_col}, Latitude, Longitude) combinations")
    else:
        log("⚠️ Required geo columns not found for duplication check.")

    log("\n🔍 Mapping Consistency Checks")
    mapping_checks = [
        (id_col, "Store Address"),
        (id_col, "City"),
        (id_col, "State")
    ]
    for k1, k2 in mapping_checks:
        if k1 in df_current.columns and k2 in df_current.columns:
            inconsistent = df_current.groupBy(k1).agg(countDistinct(k2).alias("unique_vals")).filter("unique_vals > 1").count()
            if inconsistent > 0:
                log(f"❌ Inconsistent mapping: {k1} → {k2} has {inconsistent} inconsistencies")
        else:
            log(f"⚠️ Missing column(s): {k1}, {k2}")

    log("\n🔍 Zip Code Format")
    zip_col = "Zip Code"
    zip_regex = {
        "USA": r"^\d{5}(-\d{4})?$",
        "India": r"^\d{6}$",
        "Germany": r"^\d{5}$",
        "UK": r"^[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$",
        "CA": r"^[A-Za-z]\d[A-Za-z][ -]?\d[A-Za-z]\d$"
    }.get(country, r".*")
    if zip_col in df_current.columns:
        invalid_zip = df_current.filter(~col(zip_col).rlike(zip_regex)).count()
        if invalid_zip > 0:
            log(f"❌ {invalid_zip} invalid zip codes for {country}")
    else:
        log(f"⚠️ Zip column '{zip_col}' not found.")

    log("\n🔍 Geo Bounds")
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
            log(f"❌ {lat_invalid} invalid latitudes or {lon_invalid} invalid longitudes")
    else:
        log("⚠️ Latitude or Longitude column missing.")

    log("\n🔍 Price Format")
    price_col = "Price"
    if price_col in df_current.columns:
        price_stats = df_current.withColumn("p", col(price_col).cast("float")).select("p").summary("min", "max").collect()
        if price_stats:
            log(f"ℹ️ Price Summary: min={price_stats[0]['p']}, max={price_stats[1]['p']}")
    else:
        log("⚠️ Price column not found.")

    log("\n🔍 Currency Symbol")
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
            log(f"❌ {missing_currency} rows missing currency symbol '{currency_symbol}'")
    else:
        log("⚠️ Currency check skipped (symbol or column missing).")

    return os.path.abspath(log_file)
