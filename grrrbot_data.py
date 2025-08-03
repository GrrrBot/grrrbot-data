import asyncio
import aiohttp
import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime
import logging
from decimal import Decimal
import keyboard

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("grrrbot_data")

# Configuration (adapted from bot code)
CONFIG = {
    "helius_key": os.getenv("HELIUS_API_KEY", "placeholder_helius_key"),
    "sol_mint": "So11111111111111111111111111111111111111112",
    "jup_mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "usdc_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "rpc_url": "https://rpc.helius.xyz/",
    "quote_url": "https://quote-api.jup.ag/v6/quote",
    "dexscreener_api_url": "https://api.dexscreener.com/latest/dex/pairs/solana/",
    "binance_api_url": "https://api.binance.com/api/v3/ticker/24hr",
    "slippage": 100,
    "scan_interval": 30,  # Poll every 120 seconds, tunable
    "pairs": {
        "JUP/SOL": {
            "address": "C1MgLojNLWBKADvu9BHdtgzz1oZX4dZ5zGdGcgvvW8Wz",
            "input_mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
            "output_mint": "So11111111111111111111111111111111111111112",
            "input_decimals": 6,
            "output_decimals": 9,
        },
        "SOL/USDC": {
            "address": "Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
            "input_mint": "So11111111111111111111111111111111111111112",
            "output_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "input_decimals": 9,
            "output_decimals": 6,
        },
        "JUP/USDC": {
            "address": "4Ui9QdDNuUaAGqCPcDSp191QrixLzQiLxJ1Gnqvz3szP",
            "input_mint": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
            "output_mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "input_decimals": 6,
            "output_decimals": 6,
        },
    },
    "google_sheets_credentials": "path/to/your/credentials.json",  # Do not commit this file! Use your local path.
    "google_sheets_id": "your_google_sheet_id_here",  # Replace with your actual ID locally.
}

# Spreadsheet columns (110, non-redundant)
COLUMNS = [
    "Timestamp", "Pair", "Source", "askPrice", "askQty", "bidPrice", "bidQty", "closeTime", "contextSlot",
    "count", "firstId", "highPrice", "inAmount", "inputMint", "lastId", "lastPrice", "lastQty", "lowPrice",
    "mostReliableAmmsQuoteReport_info_2AXXcN6oN9bBT5owwmTH53C7QHUXvhLeu718Kqt8rvY2",
    "mostReliableAmmsQuoteReport_info_BZtgQEyS6eXUXicYPHecYQ7PybqodXQMvkjUbP4R8mUU",
    "mostReliableAmmsQuoteReport_info_C8Gr6AUuq9hEdSYJzoEpNcdjpojPZwqG5MtQbeouNNwg",
    "mostReliableAmmsQuoteReport_info_Czfq3xZZDmsdGdUyrNLtRhGc47cXcZtLG4crryfu44zE",
    "openPrice", "openTime", "otherAmountThreshold", "outAmount", "outputMint",
    "pair_baseToken_address", "pair_baseToken_name", "pair_baseToken_symbol", "pair_chainId", "pair_dexId",
    "pair_fdv", "pair_info_header", "pair_info_imageUrl", "pair_info_openGraph",
    "pair_info_socials_0_type", "pair_info_socials_0_url", "pair_info_socials_1_type", "pair_info_socials_1_url",
    "pair_info_websites_0_label", "pair_info_websites_0_url", "pair_labels_0",
    "pair_liquidity_base", "pair_liquidity_quote", "pair_liquidity_usd", "pair_marketCap",
    "pair_pairAddress", "pair_pairCreatedAt",
    "pair_priceChange_h1", "pair_priceChange_h24", "pair_priceChange_h6", "pair_priceChange_m5",
    "pair_priceNative", "pair_priceUsd",
    "pair_quoteToken_address", "pair_quoteToken_name", "pair_quoteToken_symbol",
    "pair_txns_h1_buys", "pair_txns_h1_sells", "pair_txns_h24_buys", "pair_txns_h24_sells",
    "pair_txns_h6_buys", "pair_txns_h6_sells", "pair_txns_m5_buys", "pair_txns_m5_sells",
    "pair_url", "pair_volume_h1", "pair_volume_h24", "pair_volume_h6", "pair_volume_m5",
    "platformFee", "prevClosePrice", "priceChange", "priceChangePercent", "priceImpactPct", "quoteVolume",
    "routePlan_0_percent", "routePlan_0_swapInfo_ammKey", "routePlan_0_swapInfo_feeAmount",
    "routePlan_0_swapInfo_feeMint", "routePlan_0_swapInfo_inAmount", "routePlan_0_swapInfo_inputMint",
    "routePlan_0_swapInfo_label", "routePlan_0_swapInfo_outAmount", "routePlan_0_swapInfo_outputMint",
    "routePlan_1_percent", "routePlan_1_swapInfo_ammKey", "routePlan_1_swapInfo_feeAmount",
    "routePlan_1_swapInfo_feeMint", "routePlan_1_swapInfo_inAmount", "routePlan_1_swapInfo_inputMint",
    "routePlan_1_swapInfo_label", "routePlan_1_swapInfo_outAmount", "routePlan_1_swapInfo_outputMint",
    "routePlan_2_percent", "routePlan_2_swapInfo_ammKey", "routePlan_2_swapInfo_feeAmount",
    "routePlan_2_swapInfo_feeMint", "routePlan_2_swapInfo_inAmount", "routePlan_2_swapInfo_inputMint",
    "routePlan_2_swapInfo_label", "routePlan_2_swapInfo_outAmount", "routePlan_2_swapInfo_outputMint",
    "schemaVersion", "simplerRouteUsed", "slippageBps", "swapMode", "swapUsdValue", "symbol",
    "timeTaken", "volume", "weightedAvgPrice"
]

async def fetch_dexscreener_data(session, pair_name, pair_config):
    """Fetch Dexscreener data (adapted from scan_pair)."""
    try:
        url = f"{CONFIG['dexscreener_api_url']}{pair_config['address']}"
        async with session.get(url, timeout=5) as resp:
            data = await resp.json()
        pair = data.get("pairs", [{}])[0]
        if not pair:
            logger.warning(f"No pair data for {pair_name}")
            return {}
        
        # Flatten Dexscreener data
        row = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Pair": pair_name,
            "Source": "Dexscreener",
            "pair_priceUsd": pair.get("priceUsd"),
            "pair_priceNative": pair.get("priceNative"),
            "pair_liquidity_usd": pair.get("liquidity", {}).get("usd"),
            "pair_liquidity_base": pair.get("liquidity", {}).get("base"),
            "pair_liquidity_quote": pair.get("liquidity", {}).get("quote"),
            "pair_volume_h1": pair.get("volume", {}).get("h1"),
            "pair_volume_h6": pair.get("volume", {}).get("h6"),
            "pair_volume_h24": pair.get("volume", {}).get("h24"),
            "pair_volume_m5": pair.get("volume", {}).get("m5"),
            "pair_priceChange_h1": pair.get("priceChange", {}).get("h1"),
            "pair_priceChange_h6": pair.get("priceChange", {}).get("h6"),
            "pair_priceChange_h24": pair.get("priceChange", {}).get("h24"),
            "pair_priceChange_m5": pair.get("priceChange", {}).get("m5"),
            "pair_txns_h1_buys": pair.get("txns", {}).get("h1", {}).get("buys"),
            "pair_txns_h1_sells": pair.get("txns", {}).get("h1", {}).get("sells"),
            "pair_txns_h6_buys": pair.get("txns", {}).get("h6", {}).get("buys"),
            "pair_txns_h6_sells": pair.get("txns", {}).get("h6", {}).get("sells"),
            "pair_txns_h24_buys": pair.get("txns", {}).get("h24", {}).get("buys"),
            "pair_txns_h24_sells": pair.get("txns", {}).get("h24", {}).get("sells"),
            "pair_txns_m5_buys": pair.get("txns", {}).get("m5", {}).get("buys"),
            "pair_txns_m5_sells": pair.get("txns", {}).get("m5", {}).get("sells"),
            "pair_fdv": pair.get("fdv"),
            "pair_marketCap": pair.get("marketCap"),
            "pair_baseToken_address": pair.get("baseToken", {}).get("address"),
            "pair_baseToken_name": pair.get("baseToken", {}).get("name"),
            "pair_baseToken_symbol": pair.get("baseToken", {}).get("symbol"),
            "pair_quoteToken_address": pair.get("quoteToken", {}).get("address"),
            "pair_quoteToken_name": pair.get("quoteToken", {}).get("name"),
            "pair_quoteToken_symbol": pair.get("quoteToken", {}).get("symbol"),
            "pair_pairAddress": pair.get("pairAddress"),
            "pair_pairCreatedAt": pair.get("pairCreatedAt"),
            "pair_url": pair.get("url"),
            "pair_info_header": pair.get("info", {}).get("header"),
            "pair_info_imageUrl": pair.get("info", {}).get("imageUrl"),
            "pair_info_openGraph": pair.get("info", {}).get("openGraph"),
            "pair_info_socials_0_type": pair.get("info", {}).get("socials", [{}])[0].get("type"),
            "pair_info_socials_0_url": pair.get("info", {}).get("socials", [{}])[0].get("url"),
            "pair_info_socials_1_type": pair.get("info", {}).get("socials", [{}, {}])[1].get("type"),
            "pair_info_socials_1_url": pair.get("info", {}).get("socials", [{}, {}])[1].get("url"),
            "pair_info_websites_0_label": pair.get("info", {}).get("websites", [{}])[0].get("label"),
            "pair_info_websites_0_url": pair.get("info", {}).get("websites", [{}])[0].get("url"),
            "pair_labels_0": pair.get("labels", [None])[0],
            "pair_chainId": pair.get("chainId"),
            "pair_dexId": pair.get("dexId"),
            "schemaVersion": data.get("schemaVersion"),
        }
        logger.info(f"Fetched Dexscreener data for {pair_name}")
        return row
    except Exception as e:
        logger.error(f"Dexscreener fetch failed for {pair_name}: {e}")
        return {}

async def fetch_jupiter_data(session, pair_name, pair_config):
    """Fetch Jupiter data (adapted from trade_engine)."""
    try:
        params = {
            "inputMint": pair_config["input_mint"],
            "outputMint": pair_config["output_mint"],
            "amount": int(10000000),  # Fixed amount for consistency
            "slippageBps": CONFIG["slippage"],
        }
        start = datetime.now()
        async with session.get(CONFIG["quote_url"], params=params, timeout=5) as resp:
            quote = await resp.json()
        time_taken = (datetime.now() - start).total_seconds()
        
        # Flatten Jupiter data
        row = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Pair": pair_name,
            "Source": "Jupiter",
            "inAmount": float(quote.get("inAmount")) / 10**pair_config["input_decimals"],
            "inputMint": quote.get("inputMint"),
            "outAmount": float(quote.get("outAmount")) / 10**pair_config["output_decimals"],
            "outputMint": quote.get("outputMint"),
            "otherAmountThreshold": float(quote.get("otherAmountThreshold", 0)) / 10**pair_config["output_decimals"],
            "slippageBps": quote.get("slippageBps"),
            "swapMode": quote.get("swapMode"),
            "priceImpactPct": quote.get("priceImpactPct"),
            "contextSlot": quote.get("contextSlot"),
            "timeTaken": time_taken,
            "swapUsdValue": float(quote.get("inAmount")) / 10**pair_config["input_decimals"] * float(quote.get("priceImpactPct", 0)),
            "simplerRouteUsed": quote.get("simplerRouteUsed", False),
            "platformFee": quote.get("platformFee", {}).get("amount") if quote.get("platformFee") else None,
        }
        # Handle routePlan (up to 3 routes)
        for i, route in enumerate(quote.get("routePlan", [])[:3]):
            prefix = f"routePlan_{i}_"
            row.update({
                f"{prefix}percent": route.get("percent"),
                f"{prefix}swapInfo_ammKey": route.get("swapInfo", {}).get("ammKey"),
                f"{prefix}swapInfo_feeAmount": route.get("swapInfo", {}).get("feeAmount"),
                f"{prefix}swapInfo_feeMint": route.get("swapInfo", {}).get("feeMint"),
                f"{prefix}swapInfo_inAmount": float(route.get("swapInfo", {}).get("inAmount", 0)) / 10**pair_config["input_decimals"],
                f"{prefix}swapInfo_inputMint": route.get("swapInfo", {}).get("inputMint"),
                f"{prefix}swapInfo_label": route.get("swapInfo", {}).get("label"),
                f"{prefix}swapInfo_outAmount": float(route.get("swapInfo", {}).get("outAmount", 0)) / 10**pair_config["output_decimals"],
                f"{prefix}swapInfo_outputMint": route.get("swapInfo", {}).get("outputMint"),
            })
        # Handle mostReliableAmmsQuoteReport
        for key, value in quote.get("mostReliableAmmsQuoteReport", {}).get("info", {}).items():
            row[f"mostReliableAmmsQuoteReport_info_{key}"] = value
        logger.info(f"Fetched Jupiter data for {pair_name}")
        return row
    except Exception as e:
        logger.error(f"Jupiter fetch failed for {pair_name}: {e}")
        return {}

async def fetch_binance_data(session):
    """Fetch Binance data for SOL/USD."""
    try:
        params = {"symbol": "SOLUSDT"}
        async with session.get(CONFIG["binance_api_url"], params=params, timeout=5) as resp:
            data = await resp.json()
        row = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Pair": "SOL/USD",
            "Source": "Binance",
            "askPrice": data.get("askPrice"),
            "askQty": data.get("askQty"),
            "bidPrice": data.get("bidPrice"),
            "bidQty": data.get("bidQty"),
            "closeTime": data.get("closeTime"),
            "count": data.get("count"),
            "firstId": data.get("firstId"),
            "highPrice": data.get("highPrice"),
            "lastId": data.get("lastId"),
            "lastPrice": data.get("lastPrice"),
            "lastQty": data.get("lastQty"),
            "lowPrice": data.get("lowPrice"),
            "openPrice": data.get("openPrice"),
            "openTime": data.get("openTime"),
            "prevClosePrice": data.get("prevClosePrice"),
            "priceChange": data.get("priceChange"),
            "priceChangePercent": data.get("priceChangePercent"),
            "quoteVolume": data.get("quoteVolume"),
            "symbol": data.get("symbol"),
            "volume": data.get("volume"),
            "weightedAvgPrice": data.get("weightedAvgPrice"),
        }
        logger.info("Fetched Binance data for SOL/USD")
        return row
    except Exception as e:
        logger.error(f"Binance fetch failed: {e}")
        return {}

async def fetch_binance_spread(session):
    """Fetch Binance spread data for SOL/USD."""
    try:
        params = {"symbol": "SOLUSDT"}
        async with session.get("https://api.binance.com/api/v3/depth", params=params, timeout=5) as resp:
            data = await resp.json()
        row = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Pair": "SOL/USD",
            "Source": "Binance Spread",
            "askPrice": data.get("asks", [[None]])[0][0],
            "askQty": data.get("asks", [[None, None]])[0][1],
            "bidPrice": data.get("bids", [[None]])[0][0],
            "bidQty": data.get("bids", [[None, None]])[0][1],
            "symbol": "SOLUSDT",
        }
        logger.info("Fetched Binance spread data for SOL/USD")
        return row
    except Exception as e:
        logger.error(f"Binance spread fetch failed: {e}")
        return {}

async def save_to_tsv(rows, filename, include_header=False):
    """Save rows to TSV, with optional header."""
    df = pd.DataFrame(rows, columns=COLUMNS)
    # Replace NaN with None for TSV output
    df = df.replace({np.nan: None})
    df.to_csv(filename, sep='\t', mode='a', header=include_header, index=False)
    logger.info(f"Saved {len(rows)} rows to {filename} {'with' if include_header else 'without'} header")

async def save_to_google_sheets(rows, include_header=False):
    """Append rows to Google Sheet, with optional header, handling NaN values."""
    try:
        # Authenticate with Google Sheets API
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG["google_sheets_credentials"], scope)
        client = gspread.authorize(creds)
        
        # Open the spreadsheet
        spreadsheet = client.open_by_key(CONFIG["google_sheets_id"])
        worksheet = spreadsheet.get_worksheet(0)  # Use first sheet
        
        # Prepare data
        df = pd.DataFrame(rows, columns=COLUMNS)
        # Replace NaN with None to make JSON compliant for Google Sheets API
        df = df.replace({np.nan: None})
        # Convert all values to strings or None to ensure JSON compliance
        data = df.astype(str).replace("nan", None).values.tolist()
        
        # If including header, prepend column names
        if include_header:
            data.insert(0, COLUMNS)
        
        # Append data to the next available row
        worksheet.append_rows(data, value_input_option="RAW")
        logger.info(f"Appended {len(rows)} rows to Google Sheet {'with' if include_header else 'without'} header")
    except Exception as e:
        logger.error(f"Failed to append to Google Sheet: {e}")

async def fetch_and_save(session, tsv_filename, include_header=False):
    """Fetch data and save to TSV and Google Sheet."""
    tasks = []
    for pair_name, pair_config in CONFIG["pairs"].items():
        tasks.append(fetch_dexscreener_data(session, pair_name, pair_config))
        tasks.append(fetch_jupiter_data(session, pair_name, pair_config))
    tasks.append(fetch_binance_data(session))
    tasks.append(fetch_binance_spread(session))
    
    rows = await asyncio.gather(*tasks)
    rows = [row for row in rows if row]  # Filter out empty rows
    
    if rows:
        # Save to TSV
        await save_to_tsv(rows, tsv_filename, include_header=include_header)
        # Save to Google Sheet
        await save_to_google_sheets(rows, include_header=include_header)
    else:
        logger.warning("No data fetched")

async def main():
    """Main function to fetch and save data in a loop until Space bar is pressed."""
    # Generate timestamp for TSV filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    tsv_filename = f"[{timestamp}]grrrbot_data.tsv"
    
    # Prompt for header inclusion on first run
    response = input("create with header? (y/n): ").strip().lower()
    include_header = response == 'y'
    
    # Log action for TSV
    if os.path.exists(tsv_filename) and not include_header:
        logger.info("Appending to existing TSV without header")
    else:
        logger.info(f"Creating new TSV {'with' if include_header else 'without'} header: {tsv_filename}")
    
    logger.info("Starting data polling (press Space bar to stop)...")
    
    async with aiohttp.ClientSession() as session:
        # Loop until Space bar is pressed, fetching once per interval
        while not keyboard.is_pressed("space"):
            start_time = datetime.now()
            await fetch_and_save(session, tsv_filename, include_header=include_header)
            # Subsequent fetches should not include headers to avoid duplicates
            include_header = False
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, CONFIG["scan_interval"] - elapsed)
            logger.debug(f"Fetch took {elapsed:.2f}s, sleeping for {sleep_time:.2f}s")
            await asyncio.sleep(sleep_time)
    
    logger.info("Space bar pressed, stopping data polling...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt detected, shutting down...")
    finally:
        logger.info("Shutdown complete")