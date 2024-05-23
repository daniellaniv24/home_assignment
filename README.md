# Spark Application for Home Assignment

This repository contains a script for Spark application developed using PySpark and AWS Glue.

## Overview

The application performs various data processing tasks on CSV files stored in Amazon S3:

- Reads CSV files and parses dates.
- Calculates daily returns for stock prices.
- Computes average daily return.
- Determines the stock with the highest frequency.
- Identifies the stock with the highest volatility by year.
- Finds the top three 30-day return dates.

## Usage

1. **Setup AWS Glue Job**: Configure an AWS Glue job with the provided Spark application script.

2. **Run the Glue Job**: Execute the AWS Glue job to process data on an EMR cluster.

3. **Review Results**: Check the output CSV files in the specified S3 output folder for processed data.

## Dependencies

- PySpark
- AWS Glue Libraries
- Boto3
- Amazon S3
- Amazon EMR

