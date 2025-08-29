"""Custom client handling, including AWSCostExplorerStream base class."""

import boto3

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap


class AWSCostExplorerStream(Stream):
    """Stream class for AWSCostExplorer streams."""
    def __init__(self, tap: Tap):
        super().__init__(tap)
        # IAM Role-based auth support
        role_arn = self.config.get("role_arn")
        external_id = self.config.get("external_id")
        access_key = self.config.get("access_key")
        secret_key = self.config.get("secret_key")
        session_token = self.config.get("session_token")

        if role_arn:
            # Use STS to assume role
            sts_kwargs = {}
            if access_key and secret_key:
                sts_kwargs["aws_access_key_id"] = access_key
                sts_kwargs["aws_secret_access_key"] = secret_key
                if session_token:
                    sts_kwargs["aws_session_token"] = session_token
            sts_client = boto3.client("sts", **sts_kwargs)
            assume_role_kwargs = {
                "RoleArn": role_arn,
                "RoleSessionName": "TapAWSCostExplorerSession"
            }
            if external_id:
                assume_role_kwargs["ExternalId"] = external_id
            creds = sts_client.assume_role(**assume_role_kwargs)["Credentials"]
            self.conn = boto3.client(
                'ce',
                aws_access_key_id=creds["AccessKeyId"],
                aws_secret_access_key=creds["SecretAccessKey"],
                aws_session_token=creds["SessionToken"],
            )
        elif access_key and secret_key:
            # Fallback to direct key-based auth
            self.conn = boto3.client(
                'ce',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
            )
        else:
            raise ValueError(
                "You must provide either IAM role credentials (role_arn) or AWS access_key and secret_key for authentication."
            )
