import os
import gc
import shutil
from functools import reduce
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.storagelevel import StorageLevel

TEMP_DIR = "/kaggle/temp"
WORKING_DIR = "/kaggle/working"
DATASET_DIR = "/kaggle/input/malaysia-gtfs-history"

DATE_RANGE = {
    "start": datetime.strptime("2024-10-01", "%Y-%m-%d"),
    "end": datetime.strptime("2025-05-31", "%Y-%m-%d")
}

GTFS_FILES = [
    "agency.txt", "calendar.txt", "calendar_dates.txt", "frequencies.txt",
    "routes.txt", "stop_times.txt", "stops.txt", "trips.txt"
]

ROOT_FOLDERS = [
    "KTMB", "Prasarana_RapidBusKL", "Prasarana_RapidBusKuantan", "Prasarana_RapidBusMrtFeeder",
    "Prasarana_RapidBusPenang", "Prasarana_RapidRailKL", "myBAS_JohorBahru"
]

def merge_gtfs_feeds(input_dir, gtfs_file_list, output_dir):
    """
    Merges GTFS files from multiple dated subdirectories into unified files.
    """
    spark = SparkSession.builder.appName("Merge GTFS Files").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    unique_columns = {file: set() for file in gtfs_file_list}
    temp_dfs = {file: [] for file in gtfs_file_list}

    for subdir, _, files in os.walk(input_dir):
        subdir_name = os.path.basename(subdir)
        try:
            date_obj = datetime.strptime(subdir_name, "%Y-%m-%d")
        except ValueError:
            continue

        if DATE_RANGE["start"] <= date_obj <= DATE_RANGE["end"]:
            for file in files:
                if file in gtfs_file_list:
                    file_path = os.path.join(subdir, file)
                    df = spark.read.option("header", "true").csv(file_path)
                    df.persist(StorageLevel.MEMORY_AND_DISK)
                    unique_columns[file].update(df.columns)
                    temp_dfs[file].append((df, subdir_name))

    merged_dfs = {}
    for file in gtfs_file_list:
        aligned_dfs = []
        for df, subdir_name in temp_dfs[file]:
            for col in unique_columns[file]:
                if col not in df.columns:
                    df = df.withColumn(col, lit(None))
            df = df.withColumn("subdir", lit(subdir_name))
            ordered_cols = ["subdir"] + list(unique_columns[file])
            aligned_dfs.append(df.select(*ordered_cols))
        if aligned_dfs:
            merged_dfs[file] = reduce(lambda d1, d2: d1.unionByName(d2), aligned_dfs)

    for file, df in merged_dfs.items():
        if df is not None:
            output_path = os.path.join(output_dir, file.replace(".txt", ""))
            df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

    spark.stop()
    del merged_dfs
    gc.collect()

def rename_copy_and_cleanup(base_directory):
    """
    Renames .csv files to .txt, copies them up one level, and cleans up subdirectories.
    """
    for root, _, files in os.walk(base_directory, topdown=False):
        for file in files:
            if file.endswith(".csv"):
                current_folder = os.path.basename(root)
                parent_folder = os.path.dirname(root)
                new_file_name = f"{current_folder}.txt"
                old_file_path = os.path.join(root, file)
                new_file_path = os.path.join(parent_folder, new_file_name)
                shutil.copy2(old_file_path, new_file_path)
                print(f"Copied and renamed: {old_file_path} -> {new_file_path}")

        for file in files:
            try:
                os.remove(os.path.join(root, file))
            except Exception as e:
                print(f"Could not delete file {file}: {e}")

        if root != base_directory:
            try:
                os.rmdir(root)
            except Exception as e:
                print(f"Could not remove directory {root}: {e}")

def merge_with_feed_id(temp_dir, output_dir, gtfs_files):
    """
    Merges GTFS files from multiple feeds and adds a 'feed_id' column.
    """
    spark = SparkSession.builder.appName("Merge GTFS Feeds").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for file_name in gtfs_files:
        dfs = []
        for feed_folder in os.listdir(temp_dir):
            full_path = os.path.join(temp_dir, feed_folder, file_name)
            if os.path.exists(full_path):
                df = spark.read.option("header", "true").csv(full_path)
                df = df.withColumn("feed_id", lit(feed_folder))
                cols = df.columns
                reordered_cols = ["feed_id"] + [col for col in cols if col != "feed_id"]
                dfs.append(df.select(reordered_cols))

        if dfs:
            merged_df = reduce(lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True), dfs)
            output_path = os.path.join(output_dir, file_name.replace(".txt", ""))
            merged_df.write.mode("overwrite").option("header", "true").csv(output_path)
            print(f"Merged {file_name} with feed_id saved to {output_path}")

    spark.stop()

for root_folder in ROOT_FOLDERS:
    input_dir = os.path.join(DATASET_DIR, root_folder)
    output_dir = os.path.join(TEMP_DIR, root_folder)
    merge_gtfs_feeds(input_dir, GTFS_FILES, output_dir)

rename_copy_and_cleanup(TEMP_DIR)
merge_with_feed_id(TEMP_DIR, WORKING_DIR, GTFS_FILES)
