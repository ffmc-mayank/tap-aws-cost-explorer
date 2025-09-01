"""Stream type classes for tap-aws-cost-explorer."""

import datetime
from typing import Optional, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_aws_cost_explorer.client import AWSCostExplorerStream


class CostAndUsageWithResourcesStream(AWSCostExplorerStream):
    """Define custom stream."""

    name = "aws_cost"
    primary_keys = ["_id"]
    replication_key = "time_period_start"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("time_period_start", th.DateTimeType),
        th.Property("time_period_end", th.DateTimeType),
        th.Property("metric_name", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amount_unit", th.StringType),
        th.Property("group_keys", th.ArrayType),
        th.Property("group_definitions", th.ArrayType),
        th.Property("estimated", th.BooleanType),
        th.Property("_id", th.StringType),
    ).to_dict()

    def _get_end_date(self):
        return self.config.get(
            "end_date", datetime.datetime.today() - datetime.timedelta(days=1)
        )

    def _get_id(self, *args) -> str:
        """Generate a unique identifier based on input arguments."""
        base_string = "_".join(map(str, args))
        return base_string

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        self.authenticate()
        next_page = True
        start_date = self.get_starting_timestamp(context)
        end_date = self._get_end_date()

        kwargs = {
            "TimePeriod": {
                "Start": start_date.strftime("%Y-%m-%d"),
                "End": end_date.strftime("%Y-%m-%d"),
            },
            "Granularity": self.config.get("granularity"),
            "Metrics": self.config.get("metrics"),
            "GroupBy": self.config.get("group_by", []),
        }

        while next_page is not None:
            if type(next_page) is str:
                kwargs["NextPageToken"] = next_page

            response = self.client.get_cost_and_usage(**kwargs)
            next_page = response.get("NextPageToken")
            group_definitions = response.get("GroupDefinitions", [])

            for row in response.get("ResultsByTime"):
                time_period_start = row.get("TimePeriod", {}).get("Start", "")
                time_period_end = row.get("TimePeriod", {}).get("End", "")
                estimated = row.get("Estimated")
                # Check if we have groups
                for group in row.get("Groups", []):
                    group_keys = group.get("Keys", [])

                    for k, v in group.get("Metrics", {}).items():
                        yield {
                            "time_period_start": time_period_start,
                            "time_period_end": time_period_end,
                            "metric_name": k,
                            "amount": v.get("Amount", ""),
                            "amount_unit": v.get("Unit", ""),
                            "group_keys": group_keys,
                            "group_definitions": group_definitions,
                            "estimated": estimated,
                            "_id": self._get_id(
                                time_period_start,
                                k,  # Metric Name
                                "_".join(group_keys),  # Group Keys concatenated
                            ),
                        }

                # No grouping, process totals
                for k, v in row.get("Total", {}).items():
                    yield {
                        "time_period_start": time_period_start,
                        "time_period_end": time_period_end,
                        "metric_name": k,
                        "amount": v.get("Amount", ""),
                        "amount_unit": v.get("Unit", ""),
                        "group_keys": [],
                        "group_definitions": group_definitions,
                        "estimated": estimated,
                        "_id": self._get_id(time_period_start, k),
                    }
