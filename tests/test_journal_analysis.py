import pytest

pytestmark = pytest.mark.usefixtures("spark_test_session")
from jobs.best_journal.journal_analysis import JournalAnalysis


class TestJournalAnalysis(object):
    def test_journal_analysis(self, spark_test_session):
        df = JournalAnalysis \
            .find_journal_with_most_drugs(spark_test_session, 'test_resources/journal_analysis_input.json')

        expected_data = [
            ("Psychopharmacology", 2)
        ]
        expected_df = spark_test_session.createDataFrame(
            expected_data,
            ["journal", "mention_number"]
        )

        assert (expected_df.collect() == df.collect())
