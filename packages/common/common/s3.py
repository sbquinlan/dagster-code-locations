from io import BytesIO
from typing import Any

import dagster as dg
from dagster._utils.cached_method import cached_method
from dagster_aws.s3 import PickledObjectS3IOManager, S3Resource
from pydantic import Field


class S3IOManagerImpl(PickledObjectS3IOManager):
    def load_from_path(self, context: dg.InputContext, path) -> Any:
        try:
            return BytesIO(
                self.s3.get_object(Bucket=self.bucket, Key=path.as_posix())[
                    "Body"
                ].read()
            )
        except self.s3.exceptions.NoSuchKey:
            raise FileNotFoundError(
                f"Could not find file {path} in S3 bucket {self.bucket}"
            )

    def dump_to_path(self, context: dg.OutputContext, obj: Any, path) -> None:
        self.s3.put_object(Body=obj, Bucket=self.bucket, Key=path.as_posix())


class S3IOManager(dg.ConfigurableIOManager):
    s3_resource: dg.ResourceDependency[S3Resource]
    s3_bucket: str = Field(description="S3 bucket to use for the file manager.")
    s3_prefix: str = Field(
        default="dagster",
        description="Prefix to use for the S3 bucket for this file manager.",
    )

    @cached_method
    def inner_io_manager(self) -> S3IOManagerImpl:
        return S3IOManagerImpl(
            s3_bucket=self.s3_bucket,
            s3_session=self.s3_resource.get_client(),
            s3_prefix=self.s3_prefix,
        )

    def load_input(self, context: dg.InputContext) -> Any:
        return self.inner_io_manager().load_input(context)

    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        return self.inner_io_manager().handle_output(context, obj)
