from pathlib import Path

import dagster as dg
from common.s3 import S3IOManager  # type: ignore
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping
from dagster_aws.s3 import S3Resource

from . import assets

daily_refresh_schedule = dg.ScheduleDefinition(
    job=dg.define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

my_assets = dg.with_source_code_references(
    dg.load_assets_from_package_module(assets),
)

my_assets = dg.link_code_references_to_git(
    assets_defs=my_assets,
    git_url="https://github.com/sbquinlan/dagster-code-locations/",
    git_branch="main",
    file_path_mapping=AnchorBasedFilePathMapping(
        local_file_anchor=Path(__file__).parent,
        file_anchor_path_in_repository="packages/fuhnance/",
    ),
)

s3_client = S3Resource(
    aws_access_key_id=dg.EnvVar("OCI_ACCESS_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("OCI_SECRET_ACCESS_KEY"),
    region_name=dg.EnvVar("OCI_REGION"),
    endpoint_url=dg.EnvVar("OCI_ENDPOINT_URL"),
)

defs = dg.Definitions(
    assets=my_assets,
    schedules=[daily_refresh_schedule],
    resources={
        "oci_s3_client": s3_client,
        "oci_s3_io_manager": S3IOManager(
            s3_resource=s3_client,
            s3_bucket=dg.EnvVar("OCI_BUCKET"),
        ),
    },
)
