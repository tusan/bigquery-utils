import logging

from source.arg_parser import ArgumentParser
from source.bigquery_actions import BigQuerySwitchAction, BigQueryDisplayAction, BigQueryDeletePartitionsAction, \
    BigQueryCopyPartitionsAction
from source.bigquery_utils import BigQueryClient

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

if __name__ == "__main__":
    bigquery_client = BigQueryClient()

    parser = ArgumentParser(
        switch_action=BigQuerySwitchAction(bigquery_client),
        display_action=BigQueryDisplayAction(bigquery_client),
        delete_partitions_action=BigQueryDeletePartitionsAction(bigquery_client),
        copy_partitions=BigQueryCopyPartitionsAction(bigquery_client)
    )

    args = parser.build_parser()

    print(args.func(args))
