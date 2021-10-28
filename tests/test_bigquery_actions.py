import unittest
from unittest.mock import Mock, call

from source.bigquery_actions import (
    BigQuerySwitchAction,
    BigQueryDisplayAction,
    BigQueryDeletePartitionsAction,
    generate_range,
    BigQueryCopyPartitionsAction,
)


class TestBigQueryActionModule(unittest.TestCase):
    def test_generate_range(self):
        actual = "20211010"
        current_date = "20211014"
        expected = ["20211010", "20211011", "20211012", "20211013", "20211014"]

        self.assertEqual(expected, generate_range(actual, current_date))

    def test_generate_range_with_future_actual_date(self):
        actual = "20211018"
        current_date = "20211014"

        with self.assertRaises(ValueError):
            generate_range(actual, current_date)

    def test_generate_range_with_actual_equal_current(self):
        actual = "20211014"
        current_date = "20211014"

        with self.assertRaises(ValueError):
            generate_range(actual, current_date)


class TestBigQueryDisplayAction(unittest.TestCase):
    def test_run(self):
        args = Mock()
        args.project_id = "project_id"
        args.dataset_id = "dataset_id"
        args.table = "table"

        mock_response = Mock()
        mock_response.view_query = "SELECT date(utcInstant) as utcDate, * FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"

        bigquery_client = Mock()
        bigquery_client.extract_view_info.return_value = mock_response

        sut = BigQueryDisplayAction(bigquery_client)

        self.assertEqual("green", sut.run(args))


class TestBigQuerySwitchAction(unittest.TestCase):
    def setUp(self) -> None:
        args = Mock()
        args.project_id = "project_id"
        args.dataset_id = "dataset_id"
        args.view = "view"

        mock_response = Mock()

        bigquery_client = Mock()
        # bigquery_client.extract_view_info.return_value = mock_response

        self.args = args
        self.bigquery_client = bigquery_client
        self.mock_response = mock_response
        self.sut = BigQuerySwitchAction(self.bigquery_client)

    def test_switch_color(self):
        self.assertEqual("green", BigQuerySwitchAction._switch_color("blue"))
        self.assertEqual("blue", BigQuerySwitchAction._switch_color("green"))

    def test_switch_and_replace_color(self):
        query = "SELECT date(utcInstant) as utcDate, * " \
                "FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"

        expected = "SELECT date(utcInstant) as utcDate, * " \
                   "FROM `jobrapido-sandbox.core_versions.enriched_revenues_blue`"
        actual = BigQuerySwitchAction._switch_and_replace_color(query)

        self.assertEqual(expected, actual)

    def test_run(self):
        self.mock_response.view_query = "SELECT date(utcInstant) as utcDate, * " \
                                        "FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"

        expected_query = "SELECT date(utcInstant) as utcDate, * " \
                         "FROM `jobrapido-sandbox.core_versions.enriched_revenues_blue`"

        result = self.sut.run(self.args)

        self.assertEqual(expected_query, result)

        self.bigquery_client.update_view.assert_called_once_with(
            "project_id", "dataset_id", "view", expected_query
        )
        self.bigquery_client.delete_table.assert_not_called()
        self.bigquery_client.copy_table.assert_not_called()

    def test_run_with_rebuild(self):
        self.args.rebuild = True

        self.mock_response.view_query = "SELECT date(utcInstant) as utcDate, * " \
                                        "FROM `jobrapido-sandbox.core_versions.enriched_revenues_green`"
        expected_query = "SELECT date(utcInstant) as utcDate, * " \
                         "FROM `jobrapido-sandbox.core_versions.enriched_revenues_blue`"

        result = self.sut.run(self.args)

        self.assertEqual(expected_query, result)

        self.bigquery_client.update_view.assert_called_once_with(
            "project_id", "dataset_id", "view", expected_query
        )
        self.bigquery_client.delete_table.assert_called_once_with(
            "project_id", "dataset_id_versions", "enriched_revenues_blue"
        )
        self.bigquery_client.copy_table.assert_called_once_with(
            "project_id",
            "dataset_id_versions",
            "enriched_revenues_green",
            "enriched_revenues_blue",
        )


class TestBigQueryDeletePartitionsAction(unittest.TestCase):
    def setUp(self) -> None:
        args = Mock()
        args.project_id = "project_id"
        args.dataset_id = "dataset_id"
        args.table = "table"
        args.start_date = "20211010"
        args.end_date = "20211014"

        self.args = args
        self.bigquery_client = Mock()
        self.mock_response = Mock()
        self.sut = BigQueryDeletePartitionsAction(self.bigquery_client)

    def test_run(self):
        self.sut.run(self.args)

        calls = [
            call("project_id", "dataset_id", "table$20211010"),
            call("project_id", "dataset_id", "table$20211011"),
            call("project_id", "dataset_id", "table$20211012"),
            call("project_id", "dataset_id", "table$20211013"),
            call("project_id", "dataset_id", "table$20211014"),
        ]

        self.bigquery_client.delete_table.assert_has_calls(calls)


class TestBigQueryCopyPartitionsAction(unittest.TestCase):
    def setUp(self) -> None:
        args = Mock()
        args.dst_project_id = "dst_project_id"
        args.dataset_id = "dataset_id"
        args.src_project_id = "src_project_id"
        args.table = "table"
        args.start_date = "20211010"
        args.end_date = "20211014"

        self.args = args
        self.bigquery_client = Mock()
        self.mock_response = Mock()
        self.sut = BigQueryCopyPartitionsAction(self.bigquery_client)

    def test_run(self):
        self.sut.run(self.args)

        calls = [call(dataset='dataset_id', dst_project_id='dst_project_id', dst_table='table$20211010',
                      src_project_id='src_project_id', start_table='table$20211010'),
                 call(dataset='dataset_id', dst_project_id='dst_project_id', dst_table='table$20211011',
                      src_project_id='src_project_id', start_table='table$20211011'),
                 call(dataset='dataset_id', dst_project_id='dst_project_id', dst_table='table$20211012',
                      src_project_id='src_project_id', start_table='table$20211012'),
                 call(dataset='dataset_id', dst_project_id='dst_project_id', dst_table='table$20211013',
                      src_project_id='src_project_id', start_table='table$20211013'),
                 call(dataset='dataset_id', dst_project_id='dst_project_id', dst_table='table$20211014',
                      src_project_id='src_project_id', start_table='table$20211014')]

        self.bigquery_client.copy_table.assert_has_calls(calls)
