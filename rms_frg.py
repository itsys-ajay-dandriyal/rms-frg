# PySpark Validation Script for 1_tbl_current and 2_tbl_previous
import logging
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, length, concat_ws, lit, expr, countDistinct, round

os.makedirs("logs", exist_ok=True)
log_filename = datetime.now().strftime("logs/data_validation_%Y%m%d_%H%M%S.log")
logging.basicConfig(
    filename=log_filename,
    filemode='w',
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)


spark = SparkSession.builder.appName("DataValidation").getOrCreate()

# Load data
common_read_options = {
    "delimiter": '|',
    "header": True,
    "inferSchema": True,
    "quote": '"',
    "escape": '"',
    "multiLine": True,
    "mode": "PERMISSIVE"
}

current = spark.read.options(**common_read_options).csv("2025_07_LonghounSteakHouse.txt")
previous = spark.read.options(**common_read_options).csv("2025_06_LonghounSteakHouse_V2.txt")

# Basic Distinct -> To check the data should have only current month data
current.select("DateExtracted").distinct().show()
previous.select("DateExtracted").distinct().show()

# Date mismatch with current month
mismatched_df = current.filter(expr("month(to_date(DateExtracted)) != month(current_date())")).select("DateExtracted")
if mismatched_df.count() > 0:
    logging.warning("❌ Warning: There are records with DateExtracted not matching the current month.")
    mismatched_df.select("DateExtracted").show(5)
else:
    logging.info("All records in current month match the current date.")

# Count Checks -> If the margin is greater than 2%, it should be flagged
current_count = current.select("Store ID").count()
previous_count = previous.select("Store ID").count()
margin = abs(current_count - previous_count) / max(current_count, previous_count) * 100
if margin > 2:
    logging.warning(f"❌ Warning: Count mismatch between current and previous data exceeds 2%: {margin:.2f}%")
else:
    logging.info(f"Count mismatch between current and previous data is within 2%: {margin:.2f}%")
logging.info("Count current: %d", current_count)
logging.info("Count previous: %d", previous_count)

# Group by Store ID -> Check for outliers or unexpected values
store_counts = current.groupBy("Store ID").count()
quantiles = store_counts.agg(
    expr("percentile_approx(count, 0.25) as Q1"),
    expr("percentile_approx(count, 0.75) as Q3")
).collect()[0]
Q1 = quantiles["Q1"]
Q3 = quantiles["Q3"]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
outliers = store_counts.filter((col("count") < lower_bound) | (col("count") > upper_bound))
if outliers.count() > 0:
    logging.warning("❌ Warning: There are outliers in Store ID counts.")
    outliers.show()
else:
    logging.info("No outliers found in Store ID counts.")

# Price filtering
#Check for outliers in Price 
current.select("Price").distinct().orderBy(col("Price").desc()).show(5)

current = current.withColumn("Price", col("Price").cast("double"))
price_stats = current.select("Price").na.drop().agg(
    expr("percentile_approx(Price, 0.25) as Q1"),
    expr("percentile_approx(Price, 0.75) as Q3")
).collect()[0]
Q1 = price_stats["Q1"]
Q3 = price_stats["Q3"]
IQR = Q3 - Q1
lower_bound = Q1 - 1.5 * IQR
upper_bound = Q3 + 1.5 * IQR
price_outliers = current.filter((col("Price") < lower_bound) | (col("Price") > upper_bound))
price_outliers.select("Store ID", "Price").show(10, truncate=False)
if price_outliers.count() > 0:
    logging.warning("❌ Warning: There are outliers in Price.")
    price_outliers.show()
else:
    logging.info("No outliers found in Price.")

#Problematic if greater than 5 digits
problematic_price = current.filter(length(col("Price").cast("string")) > 5) \
    .select("Price").distinct().orderBy(col("Price").desc())
if problematic_price.count() > 0:
    logging.warning("❌ Warning: There are problematic prices.")
    problematic_price.show()
else:
    logging.info("No problematic prices found.")
previous.filter(length("Price") > 5).select("Price").distinct().orderBy("Price", ascending=False).show(5)

# Also check for Dollar signs in Price
dollar_sign_price = current.filter(col("Price").contains("$")).select("Price").distinct()
comma_price = current.filter(col("Price").contains(",")).select("Price").distinct()
if dollar_sign_price.count() > 0:
    logging.warning("❌ Warning: There are prices with dollar signs.")
    dollar_sign_price.show()
if comma_price.count() > 0:
    logging.warning("❌ Warning: There are prices with commas.")
    comma_price.show()
else:
    logging.info("No prices with dollar signs or commas found.")

# Zip Code Checks for USA
non_numeric_zips = current.filter(~col("Zip Code").rlike("^[0-9]+$"))
if non_numeric_zips.count() > 0:
    logging.warning("❌ Warning: There are non-numeric Zip Codes.")
    non_numeric_zips.show()
else:
    logging.info("All Zip Codes are numeric.")

# check the lenth of Zip Codes -> TODO: While reading the file, we can set the schema to Zip Code as String
from pyspark.sql.functions import length
problematic_zip = current.filter(length("Zip Code") != 5).select("Zip Code").distinct()
if problematic_zip.count() > 0:
    logging.warning("❌ Warning: There are Zip Codes with length not equal to 5.")
    problematic_zip.show()
else:
    logging.info("All Zip Codes have length equal to 5.")
logging.info("Numeric Zip Count: %d", current.filter(col("Zip Code").rlike("^[0-9]+$")).count())

# # Zip code format checks for Canada
# pattern_with_space = r"^[A-Za-z]\d[A-Za-z] \d[A-Za-z]\d$"   # 7-char with space
# pattern_no_space   = r"^[A-Za-z]\d[A-Za-z]\d[A-Za-z]\d$"     # 6-char no space
# with_space_df = current.filter((length("Zip Code") == 7) & col("Zip Code").rlike(pattern_with_space))
# no_space_df   = current.filter((length("Zip Code") == 6) & col("Zip Code").rlike(pattern_no_space))
# invalid_df    = current.filter(~(
#     ((length("Zip Code") == 7) & col("Zip Code").rlike(pattern_with_space)) |
#     ((length("Zip Code") == 6) & col("Zip Code").rlike(pattern_no_space))
# ))
# if invalid_df.count() > 0:
#     logging.warning("❌ Warning: Some postal codes are invalid or improperly formatted.")
#     invalid_df.select("Zip Code").distinct().show()
# else:
#     logging.info("All postal codes are valid and properly formatted.")
# format_count = 0
# if with_space_df.limit(1).count() > 0:
#     format_count += 1
# if no_space_df.limit(1).count() > 0:
#     format_count += 1
# if format_count > 1:
#     logging.warning("❌ Warning: Mixed postal code formats detected (some with space, some without).")
#     logging.info("Recommendation: Normalize to one consistent format (e.g., A1A 1A1 or A1A1A1).")
#     current.select("Zip Code").distinct().show()
# else:
#     logging.info("Consistent postal code format used across the dataset.")

# # Zip code checks for Australia or New Zealand
# australian_zip_pattern = r"^\d{4}$"
# valid_au_zips = current.filter((length("Zip Code") == 4) & col("Zip Code").rlike(australian_zip_pattern))
# invalid_au_zips = current.filter(~((length("Zip Code") == 4) & col("Zip Code").rlike(australian_zip_pattern)))
# if invalid_au_zips.count() > 0:
#     logging.warning("❌ Warning: There are invalid Australian postal codes (must be 4-digit numbers).")
#     invalid_au_zips.select("Zip Code").distinct().show()
# else:
#     logging.info("All Australian postal codes are valid (4-digit numeric).")

# has_invalid = invalid_au_zips.limit(1).count() > 0
# if has_invalid:
#     logging.warning("❌ Invalid Australian zip code formats detected.")
# else:
#     logging.info("Australian zip code format is consistent and valid across the dataset.")



# # Zip Code Checks for Germany
# german_zip_pattern = r"^\d{5}$"
# valid_german_zips = current.filter((length("Zip Code") == 5) & col("Zip Code").rlike(german_zip_pattern))
# invalid_german_zips = current.filter(~((length("Zip Code") == 5) & col("Zip Code").rlike(german_zip_pattern)))
# if invalid_german_zips.count() > 0:
#     logging.warning("❌ Warning: There are invalid German postal codes (must be 5-digit numbers).")
#     invalid_german_zips.select("Zip Code").distinct().show()
# else:
#     logging.info("All German postal codes are valid (5-digit numeric).")
# has_valid = valid_german_zips.limit(1).count() > 0
# has_invalid = invalid_german_zips.limit(1).count() > 0
# if has_invalid:
#     logging.warning("❌ Invalid German zip code formats detected.")
# else:
#     logging.info("German zip code format is consistent and valid across the dataset.")



# Geo validations -> Should be only 1 store per lat/lon 
current_rounded = current.withColumn("Latitude_rounded", round(col("Latitude"), 4)) \
                         .withColumn("Longitude_rounded", round(col("Longitude"), 4))
# === Check 1: Each Store ID should have only one (lat, lon) ===
store_to_location = current_rounded.groupBy("Store ID") \
    .agg(countDistinct("Latitude_rounded", "Longitude_rounded").alias("unique_locations")) \
    .filter("unique_locations > 1")
if store_to_location.count() > 0:
    logging.warning("❌ Some Store IDs are linked to multiple (lat, lon) locations.")
    store_to_location.show()
else:
    logging.info("Each Store ID maps to only one location.")

# === Check 2: Each (lat, lon) should have only one Store ID ===
location_to_store = current_rounded.groupBy("Latitude_rounded", "Longitude_rounded") \
    .agg(countDistinct("Store ID").alias("store_count")) \
    .filter("store_count > 1")
if location_to_store.count() > 0:
    logging.warning("❌ Some locations are linked to multiple Store IDs.")
    location_to_store.show()
else:
    logging.info("Each (lat, lon) maps to only one Store ID.")

# Distinct lat/lon
# USA
min_lat_usa = 24.396308
max_lat_usa = 49.384358
min_long_usa = -124.848974
max_long_usa = -66.885444
# # Alaska
# min_lat_ak = 51.0
# max_lat_ak = 71.538800
# min_long_ak = -179.148909
# max_long_ak = -51.216982
# Condition: inside USA or Alaska
valid_geo_condition = (
    ((col("Latitude") >= min_lat_usa) & (col("Latitude") <= max_lat_usa) &
     (col("Longitude") >= min_long_usa) & (col("Longitude") <= max_long_usa))
    # ((col("Latitude") >= min_lat_ak) & (col("Latitude") <= max_lat_ak) &
    #  (col("Longitude") >= min_long_ak) & (col("Longitude") <= max_long_ak))
)
# Filter out invalid geolocations
invalid_geo = current.filter(~valid_geo_condition)
# Show results if any
if invalid_geo.count() > 0:
    logging.warning("❌ Warning: Found stores with invalid lat/lon (outside USA & Alaska bounds).")
    invalid_geo.select("Store ID", "Latitude", "Longitude").distinct().show()
else:
    logging.info("All store coordinates fall within valid USA or Alaska bounds.")


# Geo validations for other countries
# min_lat = 42, max_lat = 83, min_long = -141, max_long = -53 # For Canada
# min_lat = 49.96, max_lat = 60.84, min_long = -8.62, max_long = 1.77 # For UK
# min_lat = -10, max_lat = -35, min_long = 113, max_long = 153 # For Australia
# min_lat = 47, max_lat = 55, min_long = 5, max_long = 15 # For Germany
# min_lat = 5, max_lat = 20, min_long = 115, max_long = 130 # For Philippines
# min_lat = 41, max_lat = 51, min_long = -5, max_long = 9 # For France

# invalid_geo = current.filter(
#     (col("Latitude") < min_lat) | (col("Latitude") > max_lat) |
#     (col("Longitude") < min_long) | (col("Longitude") > max_long)
# )
# 2. Show results if any
if invalid_geo.count() > 0:
    logging.warning("❌ Warning: Found records with out-of-bound lat/lon values.")
    invalid_geo.select("Store ID", "Latitude", "Longitude").distinct().show()
else:
    logging.info("All latitude and longitude values are within expected bounds.")

# Stores in previous but not in current -> 1% Margin
missing_store_ids = previous.select("Store ID").distinct() \
    .exceptAll(current.select("Store ID").distinct())
total_previous = previous.select("Store ID").distinct().count()
missing_count = missing_store_ids.count()
percent_missing = (missing_count / total_previous) * 100 if total_previous > 0 else 0
if percent_missing > 1:
    logging.warning("❌ Warning: %d store(s) from previous data are missing in current (%.2f%%).",
                    missing_count, percent_missing)
    missing_store_ids.show(5)
else:
    logging.info("✅ Store ID mismatch is within acceptable range (%.2f%%).", percent_missing)

previous.select("Store ID").exceptAll(current.select("Store ID")).distinct().show(5)

# Check for empty Store IDs
empty_store_ids_current = current.filter(col("Store ID") == "").select("Store ID").distinct()
if empty_store_ids_current.count() > 0:
    logging.warning("❌ Warning: There are empty Store IDs in current data.")
    empty_store_ids_current.show()
else:
    logging.info("No empty Store IDs found in current data.")


# Groupings
# Store ID consistency check -> Should be only 1 store name/address/city/state per Store ID
store_id_consistency = current.groupBy("Store ID") \
    .agg(
        countDistinct("Store Name").alias("name_count"),
        countDistinct("Store Address").alias("address_count"),
        countDistinct("City").alias("city_count"),
        countDistinct("State").alias("state_count")
    ).filter(
        (col("name_count") > 1) | 
        (col("address_count") > 1) |
        (col("city_count") > 1) |
        (col("state_count") > 1)
    )

if store_id_consistency.count() > 0:
    logging.warning("❌ Warning: Some Store IDs are linked to multiple store name/address/city/state combinations.")
    store_id_consistency.show()
else:
    logging.info("✅ Each Store ID has consistent store metadata.")

# Should have 1 records for each grouping of Store Name, Address, City, State in previous
grouping_check_2 = current.groupBy("Addition", "category", "product", "TYPE", "SubType / Size", "store id").count()
if grouping_check_2.filter(col("count") > 1).count() > 0:
    logging.warning("❌ Warning: There are multiple records for the same Addition, Category, Product, Type, SubType / Size, Store ID combination.")
    grouping_check_2.filter(col("count") > 1).show()
else:
    logging.info("All Addition, Category, Product, Type, SubType / Size, Store ID combinations are unique.")

# Price casting
# Check for uniform pricing across all stores
uniform_pricing = current.groupBy("Product", "Type", "SubType / Size") \
    .agg(countDistinct("Price").alias("distinct_prices")) \
    .filter(col("distinct_prices") == 1)

# Show product groups with only one price across all stores
if uniform_pricing.count() > 0:
    logging.warning("❌ Warning: Some product-type-subtype groups have only 1 price across all stores.")
    uniform_pricing.orderBy("Product", "Type", "SubType / Size").show(10)
else:
    logging.info("✅ All product-type-subtype groups have price variation across stores.")

# Special symbol filtering -> Symbols that should not be present in text fields
text_fields = ["competitor", "Category", "Product", "Type", "SubType / Size", "Addition", 
               "Product Id", "SubProduct Id", "SubProductId Order", "Addition Type", "Description",
               "Store Name", "Store Address", "City", "State", "Zip Code", "Country", 
               "Phone"]
symbol_concat = concat_ws("", *[col(f) for f in text_fields])
symbol_patterns = ["[*]", "[!]", "[®]", "[™]", "[?]"]
for pattern in symbol_patterns:
    symbol_matches = current.filter(symbol_concat.rlike(pattern))
    count = symbol_matches.count()
    if count > 0:
        logging.warning("❌ Warning: Found %d record(s) containing symbol pattern: %s", count, pattern)
        symbol_matches.select(*text_fields).show(5)
    else:
        logging.info("✅ No matches for symbol pattern: %s", pattern)



#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------# Additional checks for previous and before_previous data
#TODO: Automate this
# Distinct checks for all relevant columns -> Change to only competitor so that we know same competitior isnt mapped to different names
distinct_fields = ["competitor"]
for field in distinct_fields:
    current.select(field).distinct().show()
    previous.select(field).distinct().show()

# Category by Store ID Group Count -> TODO: Automate this
current.groupBy("Product", "Store Id").agg(expr("count(distinct Category)")).show(5)


# # Comparisons via views (assumes views exist in Spark SQL environment)
# spark.sql("SELECT * FROM vw_compare_without_product_id WHERE Current_Month_Price <> Last_Month_Price OR Current_Month_Price <> Last_To_Last_Month_Price").show(5)
# spark.sql("SELECT * FROM vw_compare_with_product_id WHERE Current_Month_Price <> Last_Month_Price OR Current_Month_Price <> Last_To_Last_Month_Price").show(5)

spark.stop()