"""
Function level adapters
"""
import os
import pendulum
from pendulum import DateTime
from univariate.missing_value import MissingValueDetector, MissingValueReport
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F
import logging

__all__ = [
    "get_conf_from_evn",
    "parse_spark_extra_conf",
    "get_period",
    "load_deduplicated_data",
    "detect_missing_value",
    "save_marked_missing_data_to_dwh",
]

logger = logging.getLogger()


def get_conf_from_evn():
    """
        Get conn info from env variables
    :return:
    """
    conf = dict()
    try:
        # Feature id
        conf["FEATURE_ID"] = os.getenv("FEATURE_ID")
        # Raw data period
        start_datetime = os.getenv("APP_TIME_START")  # yyyy-MM-dd'T'HH:mm:ss
        end_datetime = os.getenv("APP_TIME_END")  # yyyy-MM-dd'T'HH:mm:ss
        conf["APP_TIMEZONE"] = os.getenv("APP_TIMEZONE", default="UTC")

        conf["SPARK_EXTRA_CONF_PATH"] = os.getenv(
            "SPARK_EXTRA_CONF_PATH", default=""
        )  # [AICNS-61]
        conf["start"] = pendulum.parse(start_datetime).in_timezone(conf["APP_TIMEZONE"])
        conf["end"] = pendulum.parse(end_datetime).in_timezone(conf["APP_TIMEZONE"])

        # todo: temp patch for day resolution parsing, so later with [AICNS-59] resolution will be subdivided.
        conf["end"] = conf["end"].subtract(minutes=1)

    except Exception as e:
        print(e)
        raise e
    return conf


def parse_spark_extra_conf(app_conf):
    """
    Parse spark-default.xml style config file.
    It is for [AICNS-61] that is spark operator take only spark k/v confs issue.
    :param app_conf:
    :return: Dict (key: conf key, value: conf value)
    """
    with open(app_conf["SPARK_EXTRA_CONF_PATH"], "r") as cf:
        lines = cf.read().splitlines()
        config_dict = dict(
            list(
                filter(
                    lambda splited: len(splited) == 2,
                    (map(lambda line: line.split(), lines)),
                )
            )
        )
    return config_dict


def load_deduplicated_data(app_conf, time_col_name, data_col_name) -> DataFrame:
    """
    Validated data from DWH(Hive)
    :param app_conf:
    :param feature:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    table_name = "cleaned_dup_" + app_conf["FEATURE_ID"]
    # Inconsistent cache
    # https://stackoverflow.com/questions/63731085/you-can-explicitly-invalidate-the-cache-in-spark-by-running-refresh-table-table
    SparkSession.getActiveSession().sql(f"REFRESH TABLE {table_name}")
    query = f"""
    SELECT v.{time_col_name}, v.{data_col_name}  
        FROM (
            SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {table_name}
            ) v 
        WHERE v.date  >= {app_conf['start'].format('YYYYMMDD')} AND v.date <= {app_conf['end'].format('YYYYMMDD')} 
    """
    logger.info("load_deduplicated query: " + query)
    ts = SparkSession.getActiveSession().sql(query)
    logger.info(ts.show())
    return ts.sort(F.col(time_col_name).desc())


def __get_last_of_marked_data(
    feature_id: str, start: DateTime, time_col_name, data_col_name
) -> DataFrame:
    """

    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :return: One or Zero row dataframe
    """
    # todo: when refactoring, migrate this logic
    table_name = "cleaned_marked_missingvalue_" + feature_id
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE) PARTITIONED BY (year int, month int, day int) STORED AS PARQUET"
    )
    query = f"""
    SELECT v.{time_col_name}, v.{data_col_name}  
        FROM (
            SELECT {time_col_name}, {data_col_name}, concat(concat(cast(year as string), lpad(cast(month as string), 2, '0')), lpad(cast(day as string), 2, '0')) as date 
            FROM {table_name}
            ) v 
        WHERE v.date  < {start.format('YYYYMMDD')}
        ORDER BY {time_col_name} DESC LIMIT 1
    """
    return SparkSession.getActiveSession().sql(query)


def __detect_in_front_of(
    feature_id: str,
    start: DateTime,
    ts: DataFrame,
    time_col_name: str,
    data_col_name: str,
    period: int,
) -> DataFrame:
    """
    Detect unmarked missing values in front of first observation on time series.
    That is to say estimated interval is [APP_TIME_START, first_observed).
    It should be noticed that Returned dataframe always does not contain 'first_observed'
    :param first_observed:
    :param start_time:
    :return: DataFrame of detected missing values on [APP_TIME_START, first_observed). It does not contain 'first_observed'
    """
    last_of_previous = __get_last_of_marked_data(
        feature_id, start, time_col_name, data_col_name
    )
    if last_of_previous.count() == 0:
        # no decision
        return ts.limit(0)
    # detect in front of missing values
    first_observed = (
        ts.withColumn("min_time", F.min(time_col_name).over(Window.partitionBy()))
        .filter(F.col(time_col_name) == F.col("min_time"))
        .drop("min_time")
    )
    both_ends = last_of_previous.union(first_observed).sort(time_col_name)
    mvd = MissingValueDetector("regular")
    report: MissingValueReport = mvd.detect_missing_values(
        ts=both_ends,
        time_col_name=time_col_name,
        data_col_name=data_col_name,
        period=period,
    )
    marked_df = report.unmarked["marking_df"]
    # cut off previous(not wanted) missing values and first observation
    start_timestamp = start.int_timestamp * 1000  # pendulum datetime to unix timestamp
    previous_out = marked_df.filter(
        F.col(time_col_name) >= start_timestamp
    )  # out of previous missing values
    return previous_out.filter(
        F.col(time_col_name) != first_observed.first()[time_col_name]
    )  # out of first_observed


def __detect_between(
    ts: DataFrame, time_col_name: str, data_col_name: str, period: int
) -> DataFrame:
    """
    Detect unmarked missing values
    :param ts:
    :param time_col_name:
    :param data_col_name:
    :param period:
    :return:
    """
    mvd = MissingValueDetector("regular")
    report: MissingValueReport = mvd.detect_missing_values(
        ts=ts, time_col_name=time_col_name, data_col_name=data_col_name, period=period
    )
    return report.unmarked["marking_df"]


def __detect_behind(
    ts: DataFrame, end: DateTime, time_col_name: str, data_col_name: str, period: int
) -> DataFrame:
    """

    :param ts:
    :param end:
    :param time_col_name:
    :param data_col_name:
    :param period:
    :return:
    """
    preserve_last = True  # configurable
    return __span_from_last_data(
        ts, end, time_col_name, data_col_name, period, preserve_last
    )


def __span_from_last_data(
    ts: DataFrame,
    end: DateTime,
    time_col_name: str,
    data_col_name: str,
    period: int,
    preserve_last: bool = True,
) -> DataFrame:
    """
    Span unmarked missing values on target data interval.
    It must be noticed that if preserve_last is true (default), than last duration with period size would not be estimated to avoid collision with next day's observed data.
    Return value does not contain last observation.
    :param ts:
    :param end:
    :param period:
    :param preserve_last:
    :return:
    """
    last_timestamp = ts.sort(time_col_name).tail(1)[0][time_col_name]
    end_timestamp = end.int_timestamp * 1000
    timestamp_bound = end_timestamp - period + 1 if preserve_last else end_timestamp
    spanned_timestamps = list(range(last_timestamp, timestamp_bound, period))
    spanned_data = list(map(lambda stamp: [stamp, None], spanned_timestamps))
    spanned_df = SparkSession.getActiveSession().createDataFrame(
        data=spanned_data, schema=ts.select(time_col_name, data_col_name).schema
    )
    spanned_marked_df = spanned_df.sort(time_col_name).filter(
        F.col(time_col_name) != last_timestamp
    )
    return spanned_marked_df


def detect_missing_value(
    period: int, ts: DataFrame, time_col_name: str, data_col_name: str, app_conf
) -> DataFrame:
    """
    Detect unmarked missing values
    :param period:
    :param ts:
    :param time_col_name:
    :param data_col_name:
    :param app_conf:
    :return:
    """
    # todo: when refactoring, migrate this logic
    # base case
    if ts.count() == 0:
        last_of_previous = __get_last_of_marked_data(
            app_conf["FEATURE_ID"], app_conf["start"], time_col_name, data_col_name
        )
        if last_of_previous.count() == 0:
            # no decision
            return ts.limit(0)
        else:
            # span from previous data
            spanned_marked_df = __span_from_last_data(
                last_of_previous,
                app_conf["end"],
                time_col_name,
                data_col_name,
                period,
                preserve_last=True,
            )
            # cut off
            return spanned_marked_df.filter(
                F.col(time_col_name) >= app_conf["start"].int_timestamp * 1000
            )
    marked_in_front_of = __detect_in_front_of(
        app_conf["FEATURE_ID"],
        app_conf["start"],
        ts,
        time_col_name,
        data_col_name,
        period,
    )
    marked_between = __detect_between(ts, time_col_name, data_col_name, period)
    marked_behind = __detect_behind(
        ts, app_conf["end"], time_col_name, data_col_name, period
    )
    marked_df = marked_in_front_of.union(marked_between.union(marked_behind)).sort(
        time_col_name
    )
    return marked_df


def get_period(feature_id: str) -> int:
    """
    Get time series period produced by regularity decision maker.
    If cannot find period, Exception (Not yet concrete) will be thrown.
    :param feature_id:
    :return:
    """
    decisions = SparkSession.getActiveSession().sql(
        f"SELECT period FROM regularity_decision WHERE feature_id = {feature_id}"
    )
    period = decisions.tail(1)[0]["period"]
    return period


def append_partition_cols(ts: DataFrame, time_col_name: str, data_col_name):
    return (
        ts.withColumn("datetime", F.from_unixtime(F.col(time_col_name) / 1000))
        .select(
            time_col_name,
            data_col_name,
            F.year("datetime").alias("year"),
            F.month("datetime").alias("month"),
            F.dayofmonth("datetime").alias("day"),
        )
        .sort(time_col_name)
    )


def save_marked_missing_data_to_dwh(
    ts: DataFrame, app_conf, time_col_name: str, data_col_name: str
):
    """

    :param ts:
    :param app_conf:
    :param time_col_name:
    :param data_col_name:
    :return:
    """
    # todo: transaction
    table_name = "cleaned_marked_missingvalue_" + app_conf["FEATURE_ID"]
    SparkSession.getActiveSession().sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({time_col_name} BIGINT, {data_col_name} DOUBLE) PARTITIONED BY (year int, month int, day int) STORED AS PARQUET"
    )
    period = pendulum.period(app_conf["start"], app_conf["end"])

    # Create partition columns(year, month, day) from timestamp
    partition_df = append_partition_cols(ts, time_col_name, data_col_name)

    for date in period.range("days"):
        # Drop Partition for immutable task
        SparkSession.getActiveSession().sql(
            f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION(year={date.year}, month={date.month}, day={date.day})"
        )
    # Save
    partition_df.write.format("hive").mode("append").insertInto(table_name)
