
![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg) ![Spark](https://img.shields.io/badge/Spark-2.4.5-green)
# Consumer Complaints

## Table of Contents
1. [Project Summary](README.md#summary)
2. [Run Instruction](README.md#run)
3. [Repo structure](README.md#structure)
4. [Dataset](README.md#dataset)


## Project Summary

### Introduction
Instead of tv ad or traditional advertising board, more and more companies rely on internet for its marketing, and they want to measure their brand awareness across the internet, especially while comparing with other competitors or evaluating a specific campaign. However, the information are spread across different platforms and it could be difficult to have an big picture of a brand's exposure throughout the internet. To achieve that, this data pipeline ingests crawling information from the whole internet, ranks and compares brand popularity of the top U.S. Fast Food chains with the normalized count that they have been mentioned on internet over time. This method can also be generalized to all industries and even election campaigns to evaluate popularity and branding efficiency.

<!---### Demo -->

### Pipeline
![Pipeline](https://github.com/zhiqingrao/Common_crawl_insight/blob/master/readme_pipeline.png)

The pipeline first retrieves the Index files that contains path to WARC files for each crawling records from S3, and filters to gain the exact file path, offset, and length for each potentially related records using AWS Athena querying on url keywords for each crawling records. After shuffling the query results based on file path, spark ingests the actual WARC files that contains the crawling metadata and HTML response, processes and normalizes the data for each brands over different platforms over time, and saved the result into csv files in s3, which would be further used for visualization in Tableau.

 
## Run Instruction
1. Set up S3 bucket
2. Set up Athena and create tables with instruction [here](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/). Run Athena queries presented in the /Athena/filter.txt, store the output files into your S3 bucket. 
3. Set up AWS EMR clusters with package installation in /bootstrap/install_python_modules.sh 
4. . Run the spark job with `spark-submit --master yarn --deploy-mode client --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 --output_path {output_path} requestcount.py` {output_path} should be the path to you S3 bucket for output


## Repo structure



## Dataset
- common crawl dataset: [commoncrawl](https://commoncrawl.org/the-data/get-started/)
- common crawl index data: [index-to-warc-files-and-urls](https://commoncrawl.org/2018/03/index-to-warc-files-and-urls-in-columnar-format/)

