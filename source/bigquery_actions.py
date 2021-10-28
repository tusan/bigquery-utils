import argparse
import datetime
import logging
from typing import List

from google.cloud.bigquery import Table

from source.arg_parser import Action
from source.bigquery_utils import (
    extract_table_from_query,
    extract_production_table_color,
    BigQueryClient,
    extract_table_from_query_name_only,
)


class BigQueryDisplayAction(Action):
    def __init__(self, bigquery_client: BigQueryClient):
        super().__init__()
        self.bigquery_client = bigquery_client

    def run(self, args: argparse.Namespace):
        view = self.bigquery_client.extract_view_info(
            args.project_id, args.dataset_id, args.view
        )
        return extract_production_table_color(view.view_query)


class BigQuerySwitchAction(Action):
    def __init__(self, bigquery_client: BigQueryClient):
        super().__init__()
        self.bigquery_client = bigquery_client

    def run(self, args: argparse.Namespace):
        view = self.bigquery_client.extract_view_info(
            args.project_id, args.dataset_id, args.view
        )

        if args.rebuild is True:
            self.__rebuild_non_production_table(args.project_id, args.dataset_id, view)

        query_updated = self._switch_and_replace_color(view.view_query)
        self.bigquery_client.update_view(
            args.project_id, args.dataset_id, args.view, query_updated
        )
        return query_updated

    def __rebuild_non_production_table(
        self, project_id: str, dataset: str, view: Table
    ) -> None:
        production_table = extract_table_from_query_name_only(
            extract_table_from_query(view.view_query)
        )
        actual_production_color = extract_production_table_color(view.view_query)
        actual_non_production_color = self._switch_color(actual_production_color)
        non_production_table = production_table.replace(
            actual_production_color, actual_non_production_color
        )

        logging.info(f"deleting table {non_production_table}")
        self.bigquery_client.delete_table(
            project_id, f"{dataset}_versions", non_production_table
        )
        logging.info(
            f"table deleted... coping table {production_table} into {non_production_table}"
        )
        self.bigquery_client.copy_table(
            project_id,
            f"{dataset}_versions",
            production_table,
            non_production_table,
        )
        logging.info(f"copy done")

    @staticmethod
    def _switch_and_replace_color(query: str) -> str:
        table = extract_table_from_query(query)

        current_color = extract_production_table_color(query)
        new_color = BigQuerySwitchAction._switch_color(current_color)
        new_table_name = table.replace(current_color, new_color)
        return query.replace(table, new_table_name)

    @staticmethod
    def _switch_color(current_color: str) -> str:
        return "blue" if current_color == "green" else "green"


def generate_range(start_date_str: str, end_date_str: str) -> List[str]:
    date_format = "%Y%m%d"
    start_date = datetime.datetime.strptime(start_date_str, date_format).date()
    end_date = datetime.datetime.strptime(end_date_str, date_format).date()

    number_of_days = (end_date - start_date).days

    if number_of_days <= 0:
        raise ValueError(
            f"actual cannot be in the future: actual: {start_date_str}, current_date: {end_date_str}"
        )

    return [
        (start_date + datetime.timedelta(days=day)).strftime(date_format)
        for day in range(number_of_days + 1)
    ]


class BigQueryDeletePartitionsAction(Action):
    def __init__(self, bigquery_client: BigQueryClient):
        super().__init__()
        self.bigquery_client = bigquery_client

    def run(self, args: argparse.Namespace):
        dates = generate_range(args.start_date, args.end_date)
        logging.info(f"start deleting tables for {dates}")
        for date in dates:
            self.bigquery_client.delete_table(
                args.project_id, args.dataset_id, f"{args.table}${date}"
            )
        logging.info("deletion completed")


class BigQueryCopyPartitionsAction(Action):
    def __init__(self, bigquery_client: BigQueryClient):
        super().__init__()
        self.bigquery_client = bigquery_client

    def run(self, args: argparse.Namespace):
        dates = generate_range(args.start_date, args.end_date)
        logging.info(
            f"start copy tables from {args.src_project_id}.{args.dataset_id}.{args.table} "
            f"to {args.dst_project_id}.{args.dataset_id}.{args.table}"
        )
        for date in dates:
            self.bigquery_client.copy_table(
                src_project_id=args.src_project_id,
                dataset=args.dataset_id,
                start_table=f"{args.table}${date}",
                dst_table=f"{args.table}${date}",
                dst_project_id=args.dst_project_id,
            )
        logging.info("copy completed")
