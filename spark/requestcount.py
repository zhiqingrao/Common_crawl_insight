from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import sum
from tempfile import TemporaryFile
import re
import boto3
import nltk
import argparse
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from nltk.stem.wordnet import WordNetLemmatizer
Lem = WordNetLemmatizer()
nltk.download('wordnet')
nltk.download('words')

keywords = ['starbucks', 'domino\'s', 'mcdonald\'s', 'burger king']
platforms = ["instagram","facebook","youtube","news","blog"]

alt = [re.sub('\W+', '', word) for word in keywords]
trans = {word: word for word in keywords}
trans.update({word1: word2 for word1, word2 in zip(alt, keywords)})


def html_to_text(page):
    """Converts html page to text
    Args:
        page:  html
    Returns:
        soup.get_text(" ", strip=True):  string
    """
    try:
        encoding = EncodingDetector.find_declared_encoding(page, is_html=True)
        soup = BeautifulSoup(page, "lxml", from_encoding=encoding)
        for script in soup(["script", "style"]):
            script.extract()
        return soup.get_text(" ", strip=True)
    except:
        return ""


def fetch_process_warc_counts(rows):
    """Retrieves document from S3 Data Lake.
       The html record to be retrieved are pulled using the offset and length.
    Args:
        rows: list[string, int, int]
        For warc_filename, warc_record_offset, warc_record_length.
    """
    count = 0
    s3client = boto3.client('s3')
    for row in rows:
        if count % 10000 == 1:
            warc_path = row['warc_filename']
            offset = int(row['warc_record_offset'])
            length = int(row['warc_record_length'])
            rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
            response = s3client.get_object(Bucket='commoncrawl', Key=warc_path, Range=rangereq)
            record_stream = BytesIO(response["Body"].read())

            for record in ArchiveIterator(record_stream):
                if record.rec_type != 'response':
                    continue
                uri = record.rec_headers.get_header('WARC-Target-URI')
                page = record.content_stream().read()
                text = html_to_text(page).lower()
                date = record.rec_headers.get_header('WARC-Date')[:10]

                keywords_dict = {key: 0 for key in keywords}
                for keyword in keywords:
                    if keywords_dict[trans[keyword]] == 0 and keyword in text:
                        keywords_dict[trans[keyword]] = 1
                        for platform in platforms:
                            if platform in uri:
                                yield (trans[keyword], platform, date), 1
                        yield (trans[keyword], "other", date), 1
        count += 1

def match_schema(rows):
    for row in rows:
        yield row[0][0], row[0][1], row[0][2], row[1]

def parse_arguments(pj_name):
    """ Returns the parsed arguments from the command line """
    description = "running " + pj_name
    num_input_partitions = 400
    input_crawl = "CC-MAIN-2020-10"

    arg_parser = argparse.ArgumentParser(prog=pj_name,
                                         description=description,
                                         conflict_handler='resolve')
    arg_parser.add_argument("--input_crawl", type=str, default=input_crawl, help="month of crawl to select")
    arg_parser.add_argument("--read_input", type=bool, default=True,
                            help="if true, read in sql result from input file")
    arg_parser.add_argument("--input", type=str,
                            help="path to sql result as input file")
    arg_parser.add_argument("output", help="Output path in s3")
    arg_parser.add_argument("--num_input_partitions", type=int,
                            default=num_input_partitions,
                            help="Number of input splits/partitions")
    args = arg_parser.parse_args()
    return args

def run_job():
    pj_name = "with_query"
    args = parse_arguments(pj_name)

    # Create Spark Session
    spark = SparkSession \
        .builder \
        .appName(pj_name) \
        .getOrCreate()

    output_schema = StructType([
        StructField("brand", StringType(), True),
        StructField("domain", StringType(), True),
        StructField("day", StringType(), True),
        StructField("val", LongType(), True)
    ])
    warc_recs = None

    if not args.read_input:
        df = spark.read.load('s3://commoncrawl/cc-index/table/cc-main/warc/')
        df.createOrReplaceTempView('ccindex')
        print("-----get csv--------", args.input_crawl)
        sqldf = spark.sql(
            "SELECT url, warc_filename, warc_record_offset, warc_record_length FROM "
            "ccindex WHERE crawl = '{}' AND subset = 'warc' AND (position('facebook' "
            "in url_host_name) != 0 Or position('twitter' in url_host_name) != 0 Or "
            "position('instagram' in url_host_name) != 0  Or position('youtube' in "
            "url_host_name) != 0 Or position('news' in url) != 0 "
            "Or position('blog' in url) != 0)".format(str(args.input_crawl))) \
            .orderBy("warc_filename")
        sqldf.cache()
        sqldf.repartition(1).write.option("header","true").csv(
            "s3a://common-crawl-insight/intermediate_sql/sql_2020_10.csv")
        warc_recs = sqldf.rdd
    else:
        # input: "s3a://common-crawl-insight/intermediate_sql/sql_2020_10.csv"
        sqldf = spark.read.format("csv").option("header", True).option(
                "inferSchema", True).load(args.input)
        warc_recs = sqldf.select("url", "warc_filename", "warc_record_offset",
                                 "warc_record_length").repartition(args.num_input_partitions).rdd

    # mapPartition gets a list of (brandname, platform, date) and 1's.
    # reduceByKey combines all a's and gets word count.
    word_counts = warc_recs.mapPartitions(fetch_process_warc_counts)
    output = word_counts.reduceByKey(lambda a, b: a + b)\
                            .mapPartitions(match_schema)
    output.cache()
    df = spark.createDataFrame(output, schema=output_schema)

    # normalize each data point respect to date
    res = df.withColumn("val_pct", 100*df.val/sum("val").over(Window.partitionBy("day")))
    res = res.select(res.brand, res.domain, res.day, res.val, res.val_pct).rdd
    print("-----word_counts_all-------", res.collect())
    res.repartition(1).write.option("header","true").csv(args.output)

    spark.stop()

if __name__ == "__main__":
    run_job()

