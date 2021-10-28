import abc
import argparse
from abc import ABC


class Action(ABC):
    @abc.abstractmethod
    def run(self, args: argparse.Namespace):
        pass


class ArgumentParser:
    def __init__(
        self,
        switch_action: Action,
        display_action: Action,
        delete_partitions_action: Action,
        copy_partitions: Action,
    ):
        self.switch_action = switch_action
        self.display_action = display_action
        self.delete_partitions_action = delete_partitions_action
        self.copy_partitions_action = copy_partitions

    def build_parser(self, args=None) -> argparse.Namespace:
        parser = argparse.ArgumentParser(
            description="Helper to automate some common bigquery tasks"
        )

        subparsers = parser.add_subparsers()

        display = subparsers.add_parser("display")
        display.add_argument("--project-id", required=True)
        display.add_argument("--dataset-id", required=True)
        display.add_argument("--view", required=True)
        display.set_defaults(func=self.display_action.run)

        switch = subparsers.add_parser("switch-color")
        switch.add_argument("--project-id", required=True)
        switch.add_argument("--dataset-id", required=True)
        switch.add_argument("--view", required=True)
        switch.add_argument("--rebuild", action="store_true")
        switch.set_defaults(func=self.switch_action.run)

        delete_partitions = subparsers.add_parser("delete-partitions")
        delete_partitions.add_argument("--project-id", required=True)
        delete_partitions.add_argument("--dataset-id", required=True)
        delete_partitions.add_argument("--table", required=True)
        delete_partitions.add_argument("--start-date", required=True)
        delete_partitions.add_argument("--end-date", required=True)
        delete_partitions.set_defaults(func=self.delete_partitions_action.run)

        copy_partitions = subparsers.add_parser("copy-partitions")
        copy_partitions.add_argument("--src-project-id", required=True)
        copy_partitions.add_argument("--dst-project-id", required=True)
        copy_partitions.add_argument("--dataset-id", required=True)
        copy_partitions.add_argument("--table", required=True)
        copy_partitions.add_argument("--start-date", required=True)
        copy_partitions.add_argument("--end-date", required=True)
        copy_partitions.set_defaults(func=self.copy_partitions_action.run)

        return parser.parse_args(args)
