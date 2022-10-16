"""
Detect unmarked missing values
# will be refactored
"""

from func import *
from pyspark.sql import SparkSession


if __name__ == "__main__":
    # Initialize app
    app_conf = get_conf_from_evn()

    SparkSession.builder.config(
        "spark.hadoop.hive.exec.dynamic.partition", "true"
    ).config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")

    # [AICNS-61]
    if app_conf["SPARK_EXTRA_CONF_PATH"] != "":
        config_dict = parse_spark_extra_conf(app_conf)
        for conf in config_dict.items():
            SparkSession.builder.config(conf[0], conf[1])

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    app_conf["sql_context"] = spark

    # Get feature metadata
    data_col_name = (
        "input_data"  # todo: metadata concern or strict validation column names
    )
    time_col_name = "event_time"

    # Load data  #
    dedup_df = load_deduplicated_data(app_conf, time_col_name, data_col_name)

    # Get period
    period = get_period(app_conf["FEATURE_ID"])

    # Mark missing values
    marked_df = detect_missing_value(
        period, dedup_df, time_col_name, data_col_name, app_conf
    )

    # Save marked data to dwh
    save_marked_missing_data_to_dwh(
        ts=marked_df,
        time_col_name=time_col_name,
        data_col_name=data_col_name,
        app_conf=app_conf,
    )
    # todo: store offline report
    # todo: PRIORITY HIGH: enhance task and all relative logic to deal with already marked df or seperatly save unmarked or etc.
    spark.stop()
