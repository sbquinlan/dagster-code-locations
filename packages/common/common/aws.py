from typing import Any, Optional

import boto3
import dagster as dg
from pydantic import Field


class AWSResource(dg.ConfigurableResource):
    region_name: Optional[str] = Field(
        default=None, description="Specifies a custom region for the S3 session."
    )
    profile_name: Optional[str] = Field(
        default=None, description="Specifies a profile to connect that session."
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID to use when creating the boto3 session.",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key to use when creating the boto3 session.",
    )
    aws_session_token: Optional[str] = Field(
        default=None,
        description="AWS session token to use when creating the boto3 session.",
    )

    def resource(self, resource, **kwargs) -> Any:
        return boto3.session.Session(
            profile_name=self.profile_name,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
        ).resource(resource, **kwargs)
