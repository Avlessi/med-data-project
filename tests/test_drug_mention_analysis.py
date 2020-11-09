import time
from jobs.mentioned_drugs.drug_mention_analysis import DrugMentionAnalysis
import pytest

pytestmark = pytest.mark.usefixtures("spark_test_session")


class TestDrugMentionAnalysis(object):
    def test_drug_analysis(self, spark_test_session):
        clinical_trials_path = "test_resources/small_clinical_trials.csv"
        drugs_path = "test_resources/small_drugs.csv"
        pubmed_path = "test_resources/small_pubmed.csv"
        output_path = f"test_output-{time.time_ns()}"

        clinical_trials_df = spark_test_session.read.option('header', 'true').csv(clinical_trials_path)
        drugs_df = spark_test_session.read.option('header', 'true').csv(drugs_path)
        pubmed_df = spark_test_session.read.option('header', 'true').csv(pubmed_path)

        DrugMentionAnalysis.find_drug_mentions(clinical_trials_df, drugs_df, pubmed_df, output_path)

        result_df = spark_test_session.read.format("json").load(output_path)

        expected_data = [
            ("yes", "DIPHENHYDRAMINE", "Journal of emergency nursing", "no"),
            ("no", "DIPHENHYDRAMINE", "The Journal of pediatrics", "yes"),
            ("yes", "EPINEPHRINE", "Journal of emergency nursing\\xc3\\x28", "no"),
            ("no", "EPINEPHRINE", "The journal of allergy and clinical immunology. In practice", "yes")
        ]
        expected_df = spark_test_session.createDataFrame(
            expected_data,
            ['clinical_trials', 'drug', 'journal', 'pubmed']
        )

        assert (set(result_df.collect()) == set(expected_df.collect()))
