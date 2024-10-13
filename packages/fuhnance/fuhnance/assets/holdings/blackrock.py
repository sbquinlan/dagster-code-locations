import re
from functools import partial
from io import BytesIO
from itertools import islice
from typing import List

import dagster as dg
import pandas as pd
import requests

URI_BASE = "https://www.blackrock.com"
HOLDINGS_REGEX = re.compile(
    r"\/us\/individual\/products\/\d+\/[^/]+\/\d+.ajax\?tab=all&fileType=json"
)
BlackrockHoldingColumns = [
    "ticker",
    "name",
    "sector",
    "assetClass",
    "market",
    "weight",
    "notional",
    "shares",
    "cusip",
    "isin",
    "sedol",
    "last",
    "nation",
    "exchange",
    "currency",
    "fxRate",
    "accrualDate",
]


def _df_to_csv_bytes(df: pd.DataFrame):
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer)
    csv_buffer.seek(0)
    return csv_buffer


def _csv_bytes_to_df(
    bytes: BytesIO,
) -> pd.DataFrame:
    return pd.read_csv(bytes)


blackrock_asset = partial(
    dg.asset,
    io_manager_key="oci_s3_io_manager",
    key_prefix="blackrock",
    group_name="blackrock",
)


@blackrock_asset()
def scrape(context: dg.AssetExecutionContext):
    response = requests.get(
        f"{URI_BASE}/us/individual/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/one/one-v4"
    )
    response.raise_for_status()
    payload = response.json()
    table = payload["data"]["tableData"]
    columns = [col["name"] for col in table["columns"]]
    df = pd.DataFrame(table["data"], columns=columns)

    context.log_event(
        dg.AssetObservation(
            asset_key=context.asset_key,
            metadata={
                "num_records": len(df),
                "preview": dg.MetadataValue.md(df.head().to_markdown()),
            },
        )
    )
    return _df_to_csv_bytes(df)


@dg.op(out=dg.DynamicOut())
def funds(context: dg.OpExecutionContext, scrape: BytesIO):
    funds = _csv_bytes_to_df(scrape)
    # create a new dataframe with only ticker, isin, cusip, and name
    funds = funds[["localExchangeTicker", "fundName", "productPageUrl"]]
    funds.columns = ["ticker", "name", "url"]
    funds["ticker"] = funds["ticker"].str.strip().str.upper()
    funds["name"] = funds["name"].str.strip()
    funds.dropna(subset=["ticker"], inplace=True)
    yield from (
        dg.DynamicOutput(row, mapping_key=row["ticker"])
        for _, row in islice(funds.iterrows(), 5)
    )


def _get_holdings_uri(product_url: str):
    resp = requests.get(f"{URI_BASE}{product_url}")
    resp.raise_for_status()
    raw = resp.text

    # some funds don't have holdings like the gold trust etf
    matches = HOLDINGS_REGEX.match(raw)
    return matches[0] if matches else None


@dg.op()
def get_holdings(context: dg.OpExecutionContext, fund_record):
    print(f"Getting holdings for {fund_record}")
    holdings_uri = _get_holdings_uri(fund_record["url"])
    if not holdings_uri:
        return None

    resp = requests.get(f"{URI_BASE}{holdings_uri}")
    resp.raise_for_status()
    payload = resp.json()
    holdings = pd.DataFrame(payload["aaData"], columns=BlackrockHoldingColumns)
    holdings = holdings[["ticker", "isin", "cusip", "last", "weight"]]
    holdings["isin"] = holdings["isin"].str.strip().str.upper()
    holdings["cusip"] = holdings["cusip"].str.strip().str.upper()
    holdings["ticker"] = holdings["ticker"].str.strip().str.upper()
    return holdings


@dg.op()
def concat_holdings(context: dg.OpExecutionContext, all_holdings):
    filtered = [df for df in all_holdings if df is not None]
    if len(filtered) == 0:
        return None
    return _df_to_csv_bytes(pd.concat(filtered))


@dg.graph_asset(key_prefix="blackrock", group_name="blackrock")
def holdings(scrape: BytesIO):
    all_holdings = funds(scrape).map(get_holdings).collect()
    return concat_holdings(all_holdings)
