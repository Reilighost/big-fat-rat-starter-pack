import aiohttp
import asyncio
import re
import random
import sqlite3
from tqdm import tqdm
from datetime import datetime, timedelta
from aiohttp import TCPConnector

MAX_RETRIES = 3
PAGE_CAPACITY = 100
END = 1000

TELEGRAM_TOKEN = "you_tg_token_here"
CHAT_ID = "you_chat_id_here"
BASE_URL = f"https://aptoscan.com/transactions?ps={PAGE_CAPACITY}&q=users&f=20&p="
ADDRESS_REGEX = r"0x[a-fA-F0-9]{64}"
DATABASE_NAME = 'addresses.db'


def setup_database():
    """Set up the SQLite database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS addresses (
            address TEXT PRIMARY KEY,
            timestamp DATETIME
        );
        """)
        conn.commit()


def add_addresses_to_db(addresses):
    """Add addresses to the database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        for address in addresses:
            try:
                cursor.execute("INSERT INTO addresses (address, timestamp) VALUES (?, ?)", (address, datetime.utcnow()))
            except sqlite3.IntegrityError:
                pass
        conn.commit()


def load_existing_addresses_from_db():
    """Load existing addresses from the database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT address FROM addresses")
        return {row[0] for row in cursor.fetchall()}


async def send_telegram_message(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    async with session.post(url, json=data) as response:
        if response.status != 200:
            error_content = await response.text()
            print(f"Failed to send message to Telegram. Status: {response.status}. Error: {error_content}")

def extract_addresses(html_content):
    """Extract Aptos addresses from HTML content."""
    return re.findall(ADDRESS_REGEX, html_content)


async def get_addresses_from_page(session, url, pbar, retries=MAX_RETRIES):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                if retries:
                    return await get_addresses_from_page(session, url, pbar, retries=retries-1)
                pbar.update(1)
                return []

            content = await response.text()
            pbar.update(1)
            return extract_addresses(content)

    except Exception as e:
        if retries:
            return await get_addresses_from_page(session, url, pbar, retries=retries-1)
        pbar.update(1)
        return []


async def fetch_addresses(start, end):
    async with aiohttp.ClientSession() as session:
        with tqdm(total=end-start+1, desc="Fetching pages") as pbar:
            tasks = [get_addresses_from_page(session, BASE_URL + str(i), pbar) for i in range(start, end+1)]
            return await asyncio.gather(*tasks)


async def continuous_fetch():
    # Create TCPConnector with SSL verification disabled
    connector = aiohttp.TCPConnector(ssl=False)

    # Use the connector when creating the ClientSession
    existing_addresses = load_existing_addresses_from_db()
    start, end = 1, END

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            results = await fetch_addresses(start, end)
            new_addresses = [address for sublist in results for address in sublist if address]

            all_fetched_addresses = set(new_addresses)
            completely_unique_addresses = all_fetched_addresses - existing_addresses

            add_addresses_to_db(completely_unique_addresses)
            existing_addresses.update(completely_unique_addresses)

            current_time = (datetime.utcnow() + timedelta(hours=2)).strftime('%H:%M:%S')
            delay = random.randint(15 * 60, 40 * 60)
            next_cycle_time = (datetime.utcnow() + timedelta(seconds=delay, hours=2)).strftime('%H:%M:%S')

            report_message = (
                f"*Report Summary:*\n\n"
                f"*Cycle termination time:* `{current_time}`.\n"
                f"*Unique addresses obtained:* `{len(all_fetched_addresses)}`\n"
                f"*Addresses added to database:* `{len(completely_unique_addresses)}`\n"
                f"*Total address count:* `{len(existing_addresses)}`\n\n"
                f"Next cycle at: `{next_cycle_time}`."
            )

            await send_telegram_message(session, report_message)
            await asyncio.sleep(delay)


if __name__ == "__main__":
    setup_database()
    asyncio.run(continuous_fetch())
