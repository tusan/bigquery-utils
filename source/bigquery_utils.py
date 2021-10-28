from google.cloud import bigquery
from google.cloud.bigquery import Table, CopyJobConfig
from sql_metadata import Parser


class BigQueryClient:
    def __init__(self):
        self.google_client = bigquery.Client()

    def extract_view_info(self, project_id, dataset, table) -> Table:
        view_id = f"{project_id}.{dataset}.{table}"
        return self.google_client.get_table(view_id)

    def update_view(
        self, project_id: str, dataset: str, table: str, query_updated: str
    ) -> None:
        view_id = f"{project_id}.{dataset}.{table}"
        view = bigquery.Table(view_id)

        view.view_query = query_updated

        self.google_client.update_table(view, ["view_query"])

    def delete_table(self, project_id: str, dataset: str, table: str) -> None:
        table_id = f"{project_id}.{dataset}.{table}"
        self.google_client.delete_table(table_id, not_found_ok=True)

    def copy_table(
        self,
        src_project_id: str,
        dataset: str,
        start_table: str,
        dst_table: str,
        dst_project_id: str = None,
    ) -> None:
        start_table_id = f"{src_project_id}.{dataset}.{start_table}"
        dst_table_id = f"{dst_project_id or src_project_id}.{dataset}.{dst_table}"

        configs = CopyJobConfig()
        configs.write_disposition = "WRITE_TRUNCATE"

        job = self.google_client.copy_table(start_table_id, dst_table_id, job_config=configs)
        job.result()


def extract_table_from_query(query: str) -> str:
    if not query:
        raise ValueError("query is None")

    table = Parser(query).tables
    if len(table) > 1:
        raise ValueError(f"More than a single table in the view: {table}")

    return table[0]


def extract_table_from_query_name_only(table_name: str) -> str:
    return table_name.split(".")[-1]


def extract_production_table_color(query):
    table = extract_table_from_query(query)
    return "green" if table.endswith("green") else "blue"
