import logging
import os
import sys

from pyspark.sql.types import *
from constants.constants_holder import *

from jobs.mentioned_drugs.drug_mention_analysis import DrugMentionAnalysis
from utils.data_reader import DataReader
from utils.spark import get_spark_session


def main(argv):
    logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
    log = logging.getLogger(__name__)

    if len(argv) < 4:
        log.error("4 arguments expected: <clinical_trials_path> <drugs_path> <pubmed_path> "
                  "<output_path>")
        sys.exit(1)

    spark = get_spark_session("medical-project")

    clinical_trials_path = argv[0]
    drugs_path = argv[1]
    pubmed_path = argv[2]
    output_path = argv[3]

    log.info(f'clinical_trials_path is {clinical_trials_path}')
    log.info(f'drugs_path is {drugs_path}')
    log.info(f'pubmed_path is {pubmed_path}')
    log.info(f'output_path is {output_path}')

    clinical_trials_schema = StructType([
        StructField(CLINICAL_TRIALS_ID, StringType(), True),
        StructField(CLINICAL_TRIALS_SCIENTIFIC_TITLE, StringType(), True),
        StructField(CLINICAL_TRIALS_DATE, StringType(), True),
        StructField(CLINICAL_TRIALS_JOURNAL, StringType(), True),
    ])

    drugs_schema = StructType([
        StructField(DRUGS_ATCCODE, StringType(), True),
        StructField(DRUGS_NAME, StringType(), True)
    ])

    pubmed_schema = StructType([
        StructField(PUBMED_ID, StringType(), True),
        StructField(PUBMED_TITLE, StringType(), True),
        StructField(PUBMED_DATE, StringType(), True),
        StructField(PUBMED_JOURNAL, StringType(), True),
    ])

    # suppose that data comes in csv format
    clinical_trials_df = DataReader.read_csv_with_header_and_schema(spark, clinical_trials_schema, clinical_trials_path)

    drugs_df = DataReader.read_csv_with_header_and_schema(spark, drugs_schema, drugs_path)

    pubmed_df = DataReader.read_csv_with_header_and_schema(spark, pubmed_schema, pubmed_path)

    DrugMentionAnalysis.find_drug_mentions(clinical_trials_df, drugs_df, pubmed_df, output_path)


if __name__ == "__main__":
    main(sys.argv[1:])
