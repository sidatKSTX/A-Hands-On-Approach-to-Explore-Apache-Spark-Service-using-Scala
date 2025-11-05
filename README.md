# A-Hands-On-Approach-to-Explore-Apache-Spark-Service-using-Scala

## Project Overview
This project demonstrates Apache Spark capabilities using Scala with two main applications for data processing and analysis.

## Scala Files

### 1. FitnessTrackerDatajob.scala
A comprehensive Spark application that analyzes fitness tracker data from a CSV file.

**Key Features:**
- Loads fitness data from CSV with schema inference
- Formats activity names (removes underscores)
- Converts timestamp strings to proper timestamp format
- Analyzes calories burned by users (highest to lowest)
- Identifies most popular activities among female users
- Demonstrates both DataFrame API and SQL approaches

**Data Processing Steps:**
1. Raw data ingestion from `Fitness_tracker_data.csv`
2. Activity column formatting
3. Timestamp parsing and conversion
4. Aggregation and ranking operations
5. Gender-based filtering and analysis

### 2. WordCount.scala
A classic Spark example demonstrating RDD operations for word counting.

**Features:**
- Creates an in-memory data collection
- Converts data to RDD (Resilient Distributed Dataset)
- Performs word count using flatMap, map, and reduceByKey transformations
- Demonstrates basic Spark transformations and actions

## Data File
- **Fitness_tracker_data.csv**: Sample dataset containing user fitness metrics including user_id, age, gender, platform, activity, heartrate, calories, and timestamps

## Usage
Both applications are configured to run in local mode and include error-level logging for cleaner output.

## Walkthrough
For detailed project setup and walkthrough, visit: [Fitness Analysis Project Setup and Walkthrough](https://studv01.atlassian.net/wiki/spaces/DEB/pages/534413357/Fitness+Analysis+Project+Setup+and+Walkthrough)
