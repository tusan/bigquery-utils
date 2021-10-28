import unittest
from unittest.mock import patch, ANY

from source.bigquery_utils import (
    extract_production_table_color,
    extract_table_from_query_name_only,
    extract_table_from_query,
    BigQueryClient,
)


class TestBigQueryUtils(unittest.TestCase):
    def test_extract_query_info(self):
        query = "SELECT date(utcInstant) as utcDate, * FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"
        self.assertEqual("green", extract_production_table_color(query))

    def test_extract_query_info_should_throw_exception_for_too_much_tables(self):
        query = """SELECT
                      jr.*
                    FROM
                      `jobrapido-sandbox.core_versions.enriched_revenues_green` jr
                    JOIN
                      `jobrapido-sandbox.core.enriched_revenues` jr1
                    ON
                      jr.jobRevenueUuid =jr1.jobRevenueUuid
                    """

        with self.assertRaises(ValueError):
            extract_production_table_color(query)

    def test_extract_color_with_none_query(self):
        with self.assertRaises(ValueError):
            extract_production_table_color(None)

    def test_extract_table_from_query(self):
        query = "SELECT date(utcInstant) as utcDate, * FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"
        expected = "jobrapido-sandbox.core_versions.enriched_revenues_green"
        self.assertEqual(expected, extract_table_from_query(query))

    def test_extract_table_name_only(self):
        actual = extract_table_from_query_name_only(
            "jobrapido-sandbox.core_versions.campaign_registry_blue"
        )
        self.assertEqual("campaign_registry_blue", actual)


class TestBigQueryClient(unittest.TestCase):
    @patch("source.bigquery_utils.bigquery.Client")
    def test_copy_table(self, mocked_client):
        sut = BigQueryClient()
        sut.copy_table(
            src_project_id="src_project",
            dataset="dataset",
            start_table="start_table",
            dst_table="dst_table",
            dst_project_id="dst_project",
        )

        mocked_client().copy_table.assert_called_with(
            "src_project.dataset.start_table", "dst_project.dataset.dst_table", job_config=ANY
        )

    @patch("source.bigquery_utils.bigquery.Client")
    def test_copy_table_with_same_project(self, mocked_client):
        sut = BigQueryClient()
        sut.copy_table(
            src_project_id="src_project",
            dataset="dataset",
            start_table="start_table",
            dst_table="dst_table",
        )

        mocked_client().copy_table.assert_called_with(
            "src_project.dataset.start_table", "src_project.dataset.dst_table", job_config=ANY
        )
