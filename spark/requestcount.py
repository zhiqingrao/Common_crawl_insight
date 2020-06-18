

from __future__ import print_function

# import sys
# import re, string
# from operator import add
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
import boto3
from io import BytesIO
from warcio.archiveiterator import ArchiveIterator
from tempfile import TemporaryFile
from bs4 import BeautifulSoup
from bs4.dammit import EncodingDetector
from nltk.stem.wordnet import WordNetLemmatizer
Lem = WordNetLemmatizer()

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

class sparkJob(object):

    pj_name = "PythonFoodChain"

    output_schema = StructType([
        StructField("brand", StringType(), True),
        StructField("day", StringType(), True),
        #     StructField("domain", StringType(), True),
        StructField("val", LongType(), True)
    ])

    # keywords = ['mcdonald\'s', 'starbucks', 'subway', 'burger king', \
    #             'taco bell', 'wendy\'s', 'dunkin\' donuts', 'chick-fil-a', \
    #             'domino\'s', 'pizza hut']
    keywords = ['starbucks', 'domino\'s', 'chipotle']
    # todo: add different transformation, remove space and special char
    # todo: filter out false positive, think about wendy's
    # alt = [re.sub('\W+', '', word) for word in keywords]
    # trans = {word: word for word in keywords}
    # trans.update({word1: word2 for word1, word2 in zip(alt, keywords)})
    # keywords_dict = {key: 0 for key in keywords}

    print("----keywords---")
    # print(keywords_dict)
    # print(trans)

    def process_record(self, record):
        if record.rec_type != 'response':
            # WARC request or metadata records
            return
        content_type = record.http_headers.get_header('content-type', None)
        if content_type is None or ('html' not in content_type):
            # skip non-HTML and non_text, or unknown content types
            return
        page = record.content_stream().read()
        date = record.rec_headers.get_header('WARC-Date')[:10]
        # TODO: need decode for 'text'?
        text = html_to_text(page).lower()
        for word in self.keywords:
            if word in text:
                yield (date, word), 1

    def iterate_records(self, archive_iterator):
        """Iterate over all WARC records. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for record in archive_iterator:
            for res in self.process_record(record):
                yield res

    #         self.records_processed.add(1)

    def fetch_process_warc_records(self, iterator):
        # loop through all uri in warc.paths.gz (for CC-MAIN-2020-10)
        for uri in iterator:
            # if count % 10 != 0:
            #     count += 1
            #     continue
            s3client = boto3.client('s3')
            bucketname = 'commoncrawl'
            path = uri
            # TemporaryFile: Return a file-like object that can be used as a temporary storage area, dir=self.args.local_temp_dir
            warctemp = TemporaryFile(mode='w+b')
            # download 1 file from CC-MAIN-2020-10
            s3client.download_fileobj(bucketname, path, warctemp)
            # try/exception for failed download
            warctemp.seek(0)
            stream = warctemp
            #         ArchiveIterator parameter: no_record_parse=no_parse?
            archive_iterator = ArchiveIterator(stream)
            for res in self.iterate_records(archive_iterator):
                yield res

    args = None
    records_processed = None
    warc_input_processed = None
    warc_input_failed = None
    # description of input and output shown in --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    num_input_partitions = 60
    num_output_partitions = 7

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.pj_name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(prog=self.pj_name,
                                             description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                     " orc, json, csv")

        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                     " buffer content from S3")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()

        return args

    def add_arguments(self, parser):
        pass

    def match_schema(self, rows):
        for row in rows:
            yield row[0][0], row[0][1], row[1]

    def run_job(self):

        self.args = self.parse_arguments()
        # todo: remove local mode
        spark = SparkSession \
            .builder \
            .appName(self.pj_name) \
            .getOrCreate()
        sc = spark.sparkContext

        # input = 's3://ccinsight/input/warc.paths'
        input = 's3://ccinsight/input/test_2020_10_warc.paths'
        # input_data = sc.textFile(self.args.input, minPartitions=self.args.num_input_partitions)
        input_data = sc.textFile(input, minPartitions=self.args.num_input_partitions)
        # warc_recs = ["crawl-data/CC-MAIN-2020-10/segments/1581875149238.97/warc/CC-MAIN-20200229114448-20200229144448-00559.warc.gz"]
        # input_data = sc.parallelize(warc_recs)
        output = input_data.mapPartitions(self.fetch_process_warc_records)\
            .reduceByKey(lambda a, b: a + b)\
            .mapPartitions(self.match_schema)
        print("-----out: ", output.take(5))
        df = spark.createDataFrame(output, schema=self.output_schema)
        # todo:
        df.coalesce(1).write.csv("s3a://ccinsight/2020-feb-all_csv.csv")
        print("---------------finish write.csv---------------")
        # df.write.csv("s3a://ccinsight/2020-feb-00559_csv.csv")

        spark.stop()

if __name__ == "__main__":
    program = sparkJob()
    program.run_job()

