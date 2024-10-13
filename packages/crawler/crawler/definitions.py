import warnings
from pathlib import Path

import dagster as dg
from common.aws import AWSResource  # type: ignore
from common.sns import SNSResource  # type: ignore
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from crawler.assets.art import jeremy_miranda  # type: ignore

from . import assets

warnings.filterwarnings("ignore", category=dg.ExperimentalWarning)

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

aws_resource = AWSResource(
    region_name=dg.EnvVar("AWS_REGION"),
    aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
)

defs = dg.Definitions(
    assets=my_assets,
    sensors=[jeremy_miranda],
    resources={
        "aws": aws_resource,
        "sns": SNSResource(
            aws=aws_resource,
            topic_arn=dg.EnvVar("SNS_TOPIC_ARN"),
        ),
    },
)
