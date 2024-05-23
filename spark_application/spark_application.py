import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, udf, when, lag, avg, year, stddev
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from datetime import datetime
import boto3

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

S3_BUCKET_NAME = 'aws-glue-home-assignment-daniella'
INPUT_DATA_FILE_NAME = 'stock_prices.csv'
OUTPUT_FOLDER = 'results'


def read_csv_and_parse_dates(spark, file_name):
    """
    Read csv file from s3 bucket and parse the dates to match yyyy-MM-dd format, using a pyspark udf function.
    handle 2 cases of possible date format in the file.
    """
    df = spark.read.csv(f"s3://{S3_BUCKET_NAME}/{INPUT_DATA_FILE_NAME}", header=True, inferSchema=True)

    def parse_date(date_str):
        formats = ["%m.%d.%Y", "%m/%d/%Y"]  # Define possible date formats
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None  # Return None if no valid format is found

    parse_date_udf = udf(parse_date, StringType())
    df = df.withColumn("date", parse_date_udf(col("date")))

    return df


def save_result_to_s3(df, bucket_name, output_folder, file_name):
    """
    Saves the DataFrame as a CSV file to the specified Amazon S3 bucket.
    The DataFrame is first written to a temporary location within the bucket,
    and then copied to the final output folder with the specified file name.
    """
    file_format = "csv"
    temp_output_path = f's3://{bucket_name}/temp'
    df.repartition(1).write.mode("overwrite").option("header", "true").format(file_format).save(temp_output_path)

    prefix = "part"

    s3_resource = boto3.resource('s3')
    for obj in s3_resource.Bucket(bucket_name).objects.filter(Prefix=f"temp/{prefix}"):
        s3_resource.Object(bucket_name, f"{output_folder}/{file_name}/{file_name}.{file_format}").copy_from(
            CopySource=f"{bucket_name}/{obj.key}")
        s3_resource.Object(bucket_name, obj.key).delete()


def calculate_daily_returns(df):
    """
    For the next question, fill in the missing data and calculate daily recalculate_daily_returns1.
    1. find the last known opening and closing price for missing records (in case the null value is in the first known date - it stays null)
    2. calculate the daily return (percentage of change from closing to opening price)
    """
    df = df.orderBy("ticker", "date")

    window_ref = Window.partitionBy("ticker").orderBy("date")
    df = df.withColumn("close", when(col("close").isNull(), lag("close", 1).over(window_ref)).otherwise(col("close")))
    df = df.withColumn("open", when(col("open").isNull(), lag("open", 1).over(window_ref)).otherwise(col("open")))

    df = df.withColumn("daily_return", ((col("close") - col("open")) / col("open") * 100))

    return df


def calculate_average_daily_return(df):
    """
    Calculate average daily return for each record (i.e each date)
    """
    average_daily_return = df.groupBy("date").agg(avg("daily_return").alias("average_return")).orderBy("date")
    return average_daily_return


def calculate_highest_frequency_stock(df):
    """
    Calculate frequency for each record (closing price * volume) and then aggregate by stock and calculate the average frrequency.
    Fetch the stock with the highest average frequenct as a result
    """
    df = df.withColumn("frequency", col("close") * col("volume"))
    aggregated_df = df.groupBy("ticker").agg(avg("frequency").alias("frequency"))
    result_df = aggregated_df.orderBy(col("frequency").desc()).limit(1).select("ticker", "frequency")

    return result_df


def calculate_highest_volatility_stock_by_year(df):
    """
    Caculate highest volatility:
        1. create a year column for each record
        2. calculate standard deviation for each stock for each year
        3. fetch the stock with the hishest standard deviation (no matter which year)
    """
    df = df.withColumn("year", year(col("date")))
    aggregated_df = df.groupBy("ticker", "year").agg(stddev("daily_return").alias("standard_deviation"))
    result_df = aggregated_df.orderBy(col("standard_deviation").desc()).limit(1).select("ticker", "standard_deviation")

    return result_df


def calculate_top_three_30_day_return_dates(df):
    """
    Calculate 30 days return:
    1. fetch, if exists, the closing date of 30 days ago for each stock.
    2. filter the rows that dont have a 30 days prior closing price
    3. calcaulte the increase percentage
    4. select the top 3 dates with the highest increase percentage
    """
    window_ref = Window.partitionBy("ticker").orderBy("date")
    df = df.withColumn("close_30_days_prior", lag("close", 30).over(window_ref))

    df_filtered = df.filter(col("close_30_days_prior").isNotNull())
    df_filtered = df_filtered.withColumn("30_day_increase_percentage",
                                         (col("close") - col("close_30_days_prior")) / col("close_30_days_prior") * 100)

    top_three = df_filtered.orderBy(col("30_day_increase_percentage").desc()).select("ticker", "date",
                                                                                     "30_day_increase_percentage").limit(
        3)
    return top_three


def main():
    sc = SparkContext()
    sc.setSystemProperty("spark.sql.legacy.timeParserPolicy", "LEGACY")

    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Read CSV and parse dates into a DataFrame called stocks_data
    stocks_data = read_csv_and_parse_dates(spark, INPUT_DATA_FILE_NAME)

    # Calculate daily returns once
    stocks_data = calculate_daily_returns(stocks_data)

    # Question 1: Calculate average daily return
    average_daily_return = calculate_average_daily_return(stocks_data)
    average_daily_return.show()
    save_result_to_s3(average_daily_return, S3_BUCKET_NAME, OUTPUT_FOLDER, "average_daily_return")

    # Question 2: Calculate the highest frequency stock
    highest_frequency_stock = calculate_highest_frequency_stock(stocks_data)
    highest_frequency_stock.show()
    save_result_to_s3(highest_frequency_stock, S3_BUCKET_NAME, OUTPUT_FOLDER, "stock_frequency")

    # Question 3: Calculate the highest volatility stock by year
    highest_volatility_stock_by_year = calculate_highest_volatility_stock_by_year(stocks_data)
    highest_volatility_stock_by_year.show()
    save_result_to_s3(highest_volatility_stock_by_year, S3_BUCKET_NAME, OUTPUT_FOLDER, "stock_volatility")

    # Question 4: Calculate top three 30-day return dates
    top_three_30_day_return_dates = calculate_top_three_30_day_return_dates(stocks_data)
    top_three_30_day_return_dates.show()
    save_result_to_s3(top_three_30_day_return_dates, S3_BUCKET_NAME, OUTPUT_FOLDER, "stock_30_days_return")

    # Stop the SparkSession
    spark.stop()

    job.commit()


if __name__ == "__main__":
    main()
