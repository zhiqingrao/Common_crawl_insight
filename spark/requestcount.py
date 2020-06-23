from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
from tempfile import TemporaryFile
import re
import boto3
import nltk
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
        if count % 10 == 1:
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


def run_job():
    pj_name = "with_query"

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


    # Read csv from athena output, take rows
    sqldf = spark.read.format("csv").option("header", True).option(
        "inferSchema", True).load(
        "s3://common-crawl-insight/sql_res/Unsaved/2020/06/19/28225d51-fa38-4947-8abd-45b22a7dd3b1.csv")
    print("-----get csv--------")

    warc_recs = sqldf.select("warc_filename", "warc_record_offset",
                             "warc_record_length").orderBy("warc_filename").rdd
    print("-----sql--------", warc_recs.take(5))

    # mapPartition gets a list of words and 1's.  Filter removes all words that don't start with capital.  reduceByKey combines all a's and gets word count.  sortBy sorts by largest count to smallest.
    word_counts = warc_recs.mapPartitions(fetch_process_warc_counts)
    output = word_counts.reduceByKey(lambda a, b: a + b)\
                            .mapPartitions(match_schema)
    output.cache()
    print("-----word_counts_all-------", output.collect())

    df = spark.createDataFrame(output, schema=output_schema)
    df.repartition(1).write.csv("s3a://common-crawl-insight/output/out_feb.csv")
    print("---------------finish write.csv---------------")


    spark.stop()

if __name__ == "__main__":
    run_job()

