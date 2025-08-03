A Python script for polling and aggregating real-time data on Solana trading pairs (e.g., JUP/SOL, SOL/USDC) from sources like Dexscreener, Jupiter Aggregator, and Binance. Data is flattened into 110 non-redundant columns and saved to TSV files and Google Sheets for analysis. Built for monitoring volatility, liquidity, volumes, and quotes in a loop—perfect for trading bots or market dashboards.
This script was crafted in a quick 20-minute session with Grok (xAI's AI) as a companion tool for the GrrrBot Solana trading bot. It's async, efficient, and stops on Space bar press.
Features

Multi-Source Fetching: Pulls data from Dexscreener (pair metrics like liquidity, volume, price changes), Jupiter (swap quotes with route breakdowns), and Binance (24hr ticker + order book spread for SOL/USDT).
Flattened Output: Combines everything into a single row per fetch (up to 110 columns) for easy analysis—no nested JSON mess.
Saving Options: Appends to TSV files (timestamped) and Google Sheets (via service account).
Loop Polling: Runs every ~30 seconds (configurable), stops with Space key—great for long-term monitoring.
Error Handling: Skips failed fetches, logs issues, handles NaN/empty fields gracefully.
Extensible Pairs: Config supports adding more pairs (e.g., JUP/USDC included; uncomment for custom like J1FSLPH1/SOL).

Requirements

Python 3.8+ (tested on 3.12)
Libraries: pip install aiohttp pandas numpy gspread oauth2client keyboard
Google Sheets: Service account JSON (credentials.json) with Sheets/Drive access. Share your sheet with the service email.
Env Vars: export HELIUS_API_KEY=your_key (optional for RPC, but config uses it).

No internet-restricted packages; all are standard.
Installation

Clone the repo:
textgit clone https://github.com/GrrrBot/grrrbot-data.git
cd grrrbot-data

Install dependencies:
textpip install -r requirements.txt  # Create if needed: aiohttp pandas numpy gspread oauth2client keyboard

Set up Google Sheets credentials:

Create a service account in Google Cloud Console (enable Sheets/Drive APIs).
Download JSON as credentials.json in the script folder.
Update google_sheets_id in code to your sheet's ID (from URL).



Usage
Run the script:
textpython grrrbot_data.py

On first run: Prompt "create with header? (y/n)"—'y' adds column headers to TSV/Sheet.
It starts polling: Fetches data every 30s (configurable in scan_interval).
Stop: Press Space bar (logs "Space bar pressed").
Output: TSV file like [2025-08-03_12-34-56]grrrbot_data.tsv + appended to your Google Sheet.

Example output row (simplified):

Timestamp: 2025-08-03 12:34:56
Pair: JUP/SOL
Source: Dexscreener
pair_priceUsd: 0.35
... (full 110 columns)

Customize pairs/decimals in CONFIG dict.
Configuration
Edit CONFIG in the script:

Add/remove pairs (mint addresses, decimals).
Change scan_interval for poll frequency.
Update APIs if needed (e.g., Helius key for custom RPC).

License
MIT License—feel free to use/modify. See LICENSE file.
Credits

Built with help from Grok (xAI)—quick AI-assisted coding session!
Inspired by Solana trading tools like Jupiter/Dexscreener.

Questions? Open an issue or fork away. Happy fetching! 🚀
