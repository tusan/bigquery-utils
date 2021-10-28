import unittest
from unittest.mock import Mock

from source.arg_parser import ArgumentParser


class TestMain(unittest.TestCase):
    def setUp(self) -> None:
        self.display_mock = Mock()
        self.display_mock.run = Mock()
        self.switch_mock = Mock()
        self.switch_mock.run = Mock()
        self.delete_mock = Mock()
        self.delete_mock.run = Mock()
        self.copy_mock = Mock()
        self.copy_mock.run = Mock()
        self.sut = ArgumentParser(
            display_action=self.display_mock,
            switch_action=self.switch_mock,
            delete_partitions_action=self.delete_mock,
            copy_partitions=self.copy_mock,
        )

    def test_parse_args_for_display(self):
        parser = self.sut.build_parser(
            [
                "display",
                "--project-id=project_id",
                "--dataset-id=dataset_id",
                "--view=view",
            ]
        )
        self.assertEqual("project_id", parser.project_id)
        self.assertEqual("dataset_id", parser.dataset_id)
        self.assertEqual("view", parser.view)
        self.assertEqual(self.display_mock.run, parser.func)

    def test_parse_args_for_switch_color(self):
        parser = self.sut.build_parser(
            [
                "switch-color",
                "--project-id=project_id",
                "--dataset-id=dataset_id",
                "--view=view",
            ]
        )
        self.assertEqual("project_id", parser.project_id)
        self.assertEqual("dataset_id", parser.dataset_id)
        self.assertEqual("view", parser.view)
        self.assertEqual(self.switch_mock.run, parser.func)
        self.assertFalse(parser.rebuild)

    def test_parse_args_for_switch_rebuild_color(self):
        parser = self.sut.build_parser(
            [
                "switch-color",
                "--project-id=project_id",
                "--dataset-id=dataset_id",
                "--view=view",
                "--rebuild",
            ]
        )
        self.assertEqual("project_id", parser.project_id)
        self.assertEqual("dataset_id", parser.dataset_id)
        self.assertEqual("view", parser.view)
        self.assertEqual(self.switch_mock.run, parser.func)
        self.assertTrue(parser.rebuild)

    def test_parse_args_for_delete_action(self):
        parser = self.sut.build_parser(
            [
                "delete-partitions",
                "--project-id=project_id",
                "--dataset-id=dataset_id",
                "--table=a_table",
                "--start-date=2021-10-01",
                "--end-date=2021-10-10",
            ]
        )
        self.assertEqual("project_id", parser.project_id)
        self.assertEqual("dataset_id", parser.dataset_id)
        self.assertEqual("a_table", parser.table)
        self.assertEqual("2021-10-01", parser.start_date)
        self.assertEqual("2021-10-10", parser.end_date)
        self.assertEqual(self.delete_mock.run, parser.func)

    def test_parse_args_for_copy_action(self):
        parser = self.sut.build_parser(
            [
                "copy-partitions",
                "--dst-project-id=dst_project_id",
                "--src-project-id=src_project_id",
                "--dataset-id=dataset_id",
                "--table=a_table",
                "--start-date=2021-10-01",
                "--end-date=2021-10-10",
            ]
        )
        self.assertEqual("src_project_id", parser.src_project_id)
        self.assertEqual("dst_project_id", parser.dst_project_id)
        self.assertEqual("dataset_id", parser.dataset_id)
        self.assertEqual("a_table", parser.table)
        self.assertEqual("2021-10-01", parser.start_date)
        self.assertEqual("2021-10-10", parser.end_date)
        self.assertEqual(self.copy_mock.run, parser.func)

    def test_all(self):
        parser = self.sut.build_parser(
            "display --project-id=jobrapido-sandbox --dataset-id=core --view=enriched_revenues".split(
                " "
            )
        )

        result = parser.func(parser.project_id, parser.dataset_id, parser.view)
        self.assertIsNotNone(result)
        print(result)
