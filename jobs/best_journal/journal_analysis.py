from pyspark.sql.functions import *

from constants.constants_holder import *


class JournalAnalysis:

    @staticmethod
    def find_journal_with_most_drugs(spark, input_file):
        """
            find_journal_with_most_drugs returns DataFrame representing the journal
            which mentions the biggest number of different drugs
        """
        df = spark.read.json(input_file)
        return df \
            .select(JOURNAL, DRUGS_NAME) \
            .dropDuplicates([JOURNAL, DRUGS_NAME]) \
            .groupBy(JOURNAL) \
            .agg(count("*").alias('mention_number')) \
            .orderBy(desc('mention_number')) \
            .limit(1)
