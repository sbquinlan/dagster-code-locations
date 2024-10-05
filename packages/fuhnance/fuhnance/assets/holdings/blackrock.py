import base64
import json
import os
from io import BytesIO
import re
import pandas as pd
import requests
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

URI_BASE = "https://www.blackrock.com"
HOLDINGS_REGEX = re.compile(r"\/us\/individual\/products\/\d+\/[^/]+\/\d+.ajax\?tab=all&fileType=json")
BlackrockHoldingColumns = [
  'ticker',
  'name',
  'sector',
  'assetClass',
  'market',
  'weight',
  'notional',
  'shares',
  'cusip',
  'isin',
  'sedol',
  'last',
  'nation',
  'exchange',
  'currency',
  'fxRate',
  'accrualDate',
]

@asset(group_name="blackrock")
def blackrock_funds_scrape():
  response = requests.get(
    f"{URI_BASE}/us/individual/product-screener/product-screener-v3.jsn?dcrPath=/templatedata/config/product-screener-v3/data/en/one/one-v4"
  )
  response.raise_for_status()
  payload = response.json()
  table = payload["data"]["tableData"]
  columns = [col["name"] for col in table["columns"]]
  df = pd.DataFrame(table["data"], columns=columns)
  df.to_csv("data/blackrock_scrape.csv")

  return MaterializeResult(
    metadata={
      "num_records": len(df),
      "preview": MetadataValue.md(df.head().to_markdown()),
    }
  )

@asset(deps=[blackrock_funds_scrape], group_name="blackrock")
def blackrock_funds(context: AssetExecutionContext) -> MaterializeResult:
  with open("data/blackrock_scrape.csv", "r") as f:
    funds = pd.read_csv(f)
  # create a new dataframe with only ticker, isin, cusip, and name
  funds = funds[["localExchangeTicker", "fundShortName"]]
  funds.columns = ["ticker", "name"]
  funds["isin"] = ""
  funds["cusip"] = ""
  funds["ticker"] = funds["ticker"].str.strip().str.upper()
  funds["name"] = funds["name"].str.strip()

  funds.dropna(subset=["ticker"], inplace=True)

  funds.to_csv("data/funds.csv", index=False)
  return MaterializeResult(
    metadata={
      "num_records": len(funds),
      "preview": MetadataValue.md(funds.head().to_markdown()),
    }
  )

def _get_holdings_uri(product_url: str):
  resp = requests.get(f"{URI_BASE}{product_url}")
  resp.raise_for_status()
  raw = resp.text
  
  # some funds don't have holdings like the gold trust etf
  matches = HOLDINGS_REGEX.match(raw)
  return matches[0] if matches else None

def _get_holdings(fund_record):
  holdings_uri = _get_holdings_uri(fund_record["productPageUrl"])
  if not holdings_uri:
    return []
  
  resp = requests.get(f"{URI_BASE}{holdings_uri}")
  resp.raise_for_status()
  payload = resp.json()
  holdings = pd.DataFrame(payload["aaData"], columns=BlackrockHoldingColumns)
  holdings = holdings[["ticker", "isin", "cusip", "last", "weight"]]
  holdings["isin"] = holdings["isin"].str.strip().str.upper()
  holdings["cusip"] = holdings["cusip"].str.strip().str.upper()
  holdings["ticker"] = holdings["ticker"].str.strip().str.upper()
  return holdings

@asset(deps=[blackrock_funds], group_name="blackrock")
def blackrock_holdings(context: AssetExecutionContext) -> MaterializeResult:
  with open("data/funds.csv", "r") as f:
    funds = pd.read_csv(f)
  df = funds.apply(_get_holdings, axis=1)
  df.to_csv("data/holdings.csv", index=False)
  return MaterializeResult(
    metadata={
      "num_records": len(df),
      "preview": MetadataValue.md(df.head().to_markdown()),
    }
  )
