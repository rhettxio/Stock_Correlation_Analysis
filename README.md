![Python 3.7](https://img.shields.io/badge/python-v3.7-blue)
![Spark](https://img.shields.io/badge/Spark-2.4.5-green)

# Stock_Correlation_Analysis
This is a project during the Insight Data Engineering Program 2020B Session in New York.

# Table of Contents
1. [Instructions](README.md#Instructions)
2. [Data Pipeline](README.md#Data-Pipeline)
3. [Demo](README.md#Demo)
4. [Prerequisites](README.md#Prerequisites)
5. [Instructions](README.md#Instructions)

# Introduction
This project provides the correlation analysis of thousands of stocks over 20 years by using the [pearson correlation](https://en.wikipedia.org/wiki/Pearson_correlation_coefficient) of asset returns. Correlation can reveal stocks that have moved up or down in percentage terms either together or opposite or even no correlations. Understanding the correlation between different assets help people to create diversified portfolios to hedge the risk.

# Data Pipeline
![pipeline](https://github.com/rhettxio/Stock_Correlation_Analysis/blob/master/docs/pipeline.png)
The whole pipeline can be split into two parts. First part is data pre-processing, the data is read and pre-processed by Spark installed in EMR cluster, after this step the return data of all tickers will be derived and split into multiple tables then load in the RDS Postgresql database. Second part is actived when a user ask for correlation analysis, the Spark set up on the same EMR cluster as the first part, will extract the return data of selected tickers and time window defined by user, and returns a correlation matrix.

# Demo
![Demo](https://media.giphy.com/media/S939VwsrwtRbg1eszW/giphy.gif)

# Prerequisites
1. [Apache Spark 2.4.5](https://spark.apache.org/docs/2.4.5/)
2. [Amazon AWS](https://aws.amazon.com/)
	1. [EMR](https://aws.amazon.com/emr/?nc=sn&loc=0&whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
	2. [S3](https://aws.amazon.com/s3/)
	3. [RDS](https://aws.amazon.com/rds/)
3. Runing on a Unix or Linux machine

# Instructions

## AWS
1. AWS: Create an AWS account.
2. S3: Set up a S3 bucket.
3. EMR: Create EMR cluster emr-5.30.1. Software: Hadoop 2.8.5, Hive 2.3.6, Spark 2.4.5, Zeppelin 0.8.2. Hardware: 1 master(m5.xlarge) and 2 workers(m5.xlarge).
4. PostgreSQL: Create a PostgreSQL database on RDS.

## Dataset
* The dataset is download from Yahoo finance and stored in S3 bucket.

## Pre-processing Data
* First time running needs to execute `run_prep.sh` in [run folder](https://github.com/rhettxio/Stock_Correlation_Analysis/tree/master/run).

## Correlation Anlysis
* Once `run_prep.sh` is done, a `table_tickers_dict.txt` file will be created, and pre-prossed return data is stored in the PostgreSQL database.
* Customize the ticker symbols and start_date end_date in `settings.py` in [user_input](https://github.com/rhettxio/Stock_Correlation_Analysis/tree/master/user_input) for correlation calculation.
* `run.sh` will store the results into PostgreSQL database.

## Display
* Connect Tableau with the public DNS of PostgreSQL database.
* Select Table: "table_rt" and "corrmatrix" for display.
