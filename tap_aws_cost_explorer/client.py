"""Custom client handling, including AWSCostExplorerStream base class."""

import os
import boto3

from singer_sdk.streams.core import Stream
from singer_sdk.tap_base import Tap


class AWSCostExplorerStream(Stream):
    """Stream class for AWSCostExplorer streams."""
    def __init__(self, tap: Tap):
        super().__init__(tap)
        self._client = None

    @property
    def client(self):
        """Property to access client object."""
        if not self._client:
            raise Exception("Client not yet initialized")
        return self._client

    def _create_client(self, config):
        aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
            "AWS_SECRET_ACCESS_KEY"
        )
        aws_session_token = config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )
        aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
        aws_endpoint_url = config.get("aws_endpoint_url")
        aws_region_name = config.get("aws_region_name")

        # AWS credentials based authentication
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
                region_name=aws_region_name,
            )
        # AWS Profile based authentication
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)
        if aws_endpoint_url:
            ce = aws_session.client("ce", endpoint_url=aws_endpoint_url)
        else:
            ce = aws_session.client("ce")
        return ce

    def authenticate(self):
        self._client = self._create_client(self.config)
