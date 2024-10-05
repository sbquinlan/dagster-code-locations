from pathlib import Path

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    link_code_references_to_git,
    load_assets_from_package_module,
    with_source_code_references,
    ExperimentalWarning,
)
from dagster._core.definitions.metadata.source_code import AnchorBasedFilePathMapping

from . import assets
import warnings
warnings.filterwarnings("ignore", category=ExperimentalWarning)

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

my_assets = with_source_code_references(
  load_assets_from_package_module(assets),
)

my_assets = link_code_references_to_git(
  assets_defs=my_assets,
  git_url="https://github.com/sbquinlan/dagster-code-locations/",
  git_branch="main",
  file_path_mapping=AnchorBasedFilePathMapping(
    local_file_anchor=Path(__file__).parent,
    file_anchor_path_in_repository="packages/fuhnance/",
  ),
)

defs = Definitions(
    assets=my_assets,
    schedules=[daily_refresh_schedule],
)
