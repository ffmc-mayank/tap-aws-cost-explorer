"""AWSCostExplorer tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_aws_cost_explorer.streams import (
    CostAndUsageWithResourcesStream,
)
STREAM_TYPES = [
    CostAndUsageWithResourcesStream,
]


class TapAWSCostExplorer(Tap):
    """AWSCostExplorer tap class."""
    name = "tap-aws-cost-explorer"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_key",
            th.StringType,
            description="Your AWS Account Access Key. Required if not using IAM role."
        ),
        th.Property(
            "secret_key",
            th.StringType,
            description="Your AWS Account Secret Key. Required if not using IAM role."
        ),
        th.Property(
            "session_token",
            th.StringType,
            description="Your AWS Account Session Token if required for authentication."
        ),
        th.Property(
            "role_arn",
            th.StringType,
            description="Optional. ARN of the IAM Role to assume for authentication. If provided, will use role-based auth."
        ),
        th.Property(
            "external_id",
            th.StringType,
            description="Optional. External ID to use when assuming the IAM role."
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=True,
            description="The start date for retrieving Amazon Web Services cost."
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The end date for retrieving Amazon Web Services cost."
        ),
        th.Property(
            "granularity",
            th.StringType,
            required=True,
            description="Sets the Amazon Web Services cost granularity to MONTHLY or DAILY , or HOURLY."
        ),
        th.Property(
            "metrics",
            th.ArrayType(th.StringType),
            required=True,
            description="Which metrics are returned in the query. Valid values are AmortizedCost, BlendedCost, NetAmortizedCost, NetUnblendedCost, NormalizedUsageAmount, UnblendedCost, and UsageQuantity."
        ),
        th.Property(
            "group_by",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Type", th.StringType, required=True, description="Type of grouping: 'TAG' or 'DIMENSION'"),
                    th.Property("Key", th.StringType, required=True, description="Key to group by, e.g., 'Application' for TAGs or 'REGION' for DIMENSION")
                )
            ),
            description="Optional. The dimensions to group the results by. Example: [{'Type': 'TAG', 'Key': 'Application'}, {'Type': 'DIMENSION', 'Key': 'REGION'}]"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
