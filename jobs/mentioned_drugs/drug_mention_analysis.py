from pyspark.sql.functions import *

from constants.constants_holder import *


class DrugMentionAnalysis(object):
    def __init__(self, clinical_trials_path, drugs_path, pubmed_path, output_path):
        self.clinical_trials_path = clinical_trials_path
        self.drugs_path = drugs_path
        self.pubmed_path = pubmed_path
        self.output_path = output_path

    @staticmethod
    def transform_title(col_name, new_col_name):
        return lambda df: (
            df
                .withColumn(new_col_name, explode(split(col(col_name), '\\W+')))
                .withColumn(new_col_name, upper(col(new_col_name)))
        )

    @staticmethod
    def filter_col_lenth(col_name, min_len, max_len):
        return lambda df: (
            df
                .where(length(col(col_name)) >= min_len)
                .where(length(col(col_name)) <= max_len)
        )

    @staticmethod
    def find_drug_mentions(clinical_trials_df, drugs_df, pubmed_df, output_path):
        """
            find_drug_mentions looks for drug mentions according to rules:
            - drug is considered as mentioned in PubMed article or in clinical test if it's mentioned in publication title.
            - drug is considered as mentioned bu journal if it is mentioned in publication issued by this journal.
            In output we get json file with columns having information like this:
             {"drug":"BETAMETHASONE","journal":"Hôpitaux Universitaires de Genève","clinical_trials":"yes","pubmed":"no"}
        """
        trials_word_col = 'trials_word'
        pubmed_word_col = 'pubmed_word'

        min_drug_length = int(drugs_df.agg(min(length(col(DRUGS_NAME)))).collect()[0][0])
        max_drug_length = int(drugs_df.agg(max(length(col(DRUGS_NAME)))).collect()[0][0])

        # filtering on drug length helps us to drastically
        # decrease a number of rows after explode
        trials = clinical_trials_df \
            .transform(DrugMentionAnalysis.transform_title(CLINICAL_TRIALS_SCIENTIFIC_TITLE, trials_word_col)) \
            .transform(DrugMentionAnalysis.filter_col_lenth(trials_word_col, min_drug_length, max_drug_length)) \
            .withColumn(pubmed_word_col, expr("null"))

        pubmed = pubmed_df \
            .transform(DrugMentionAnalysis.transform_title(PUBMED_TITLE, pubmed_word_col)) \
            .transform(DrugMentionAnalysis.filter_col_lenth(pubmed_word_col, min_drug_length, max_drug_length)) \
            .withColumn(trials_word_col, expr("null"))

        drugs_df \
            .join(trials, col(DRUGS_NAME) == col(trials_word_col)) \
            .select(DRUGS_NAME, trials_word_col, pubmed_word_col, JOURNAL) \
            .union(drugs_df
                   .join(pubmed, col(DRUGS_NAME) == col(pubmed_word_col))
                   .select(DRUGS_NAME, trials_word_col, pubmed_word_col, JOURNAL)
                   ) \
            .withColumn('clinical_trials', when(col(trials_word_col).isNotNull(), lit("yes"))
                        .otherwise(lit("no"))) \
            .withColumn('pubmed', when(col(pubmed_word_col).isNotNull(), lit("yes"))
                        .otherwise(lit("no"))) \
            .drop(trials_word_col, pubmed_word_col) \
            .orderBy(DRUGS_NAME) \
            .coalesce(1) \
            .write \
            .format('json') \
            .save(output_path)
