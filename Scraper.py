import aiohttp
import asyncio
import re
import requests

from datetime import datetime
from lxml import html
from fake_useragent import UserAgent
from aiohttp.client_exceptions import ClientConnectorError
from tqdm import tqdm


TELEGRAM_TOKEN = "You_token_here"  # Replace with your bot token
CHAT_ID = "You_chat_id_here"  # Replace with your chat ID
MAX_RETRIES = 3  # You can adjust this as per your requirement.
END = 329373795
DEFAULT_START_POINT = 1

fix = 0
address_counter = 0
reports = 69

requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates")


async def send_telegram_message(session, message, error_counter):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "MarkdownV2"
    }
    try:
        async with session.post(url, json=data) as response:
            if response.status != 200:
                print(f"Failed to send message to Telegram. Status: {response.status}")
    except ClientConnectorError as e:
        print(f"Connection error when sending message to Telegram: {e}")
        error_counter += 1

async def get_address(session, url, retries=MAX_RETRIES, error_counter=None):
    headers = {'User-Agent': ua.random}

    try:
        async with session.get(url, headers=headers) as response:
            if response.status != 200:
                if retries:
                    return await get_address(session, url, retries=retries-1)
                print(f"Failed for {url} after {MAX_RETRIES} retries.")

                # Save the failed URL to the file
                with open('failed_urls.txt', 'a') as f:
                    f.write(url + '\n')

                return None, None

            content = await response.text()
            tree = html.fromstring(content)

            first_address_list = tree.xpath('//*[@id="ContentPlaceHolder1_divUserPanel"]/div[1]/div[2]/a/text()')
            second_address_list = tree.xpath('//*[@id="ContentPlaceHolder1_divUserPanel"]/div[2]/div[2]/a/text()')

            first_address = first_address_list[0] if first_address_list else None
            second_address = second_address_list[0] if second_address_list else None

            return first_address, second_address


    except Exception as e:

        if retries:
            print(f"Retrying for {url} due to error: {e}. Remaining retries: {retries - 1}")

            return await get_address(session, url, retries=retries - 1, error_counter=error_counter)

        print(f"Failed for {url} after {MAX_RETRIES} retries due to error: {e}.")

        if error_counter is not None:
            error_counter += 1

        return None, None


async def main(start, end):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=False)) as session:
        tasks = [get_address(session, f"https://aptoscan.com/version/{i}") for i in range(start, end)]
        return await asyncio.gather(*tasks)

async def clean_txt_file(session, filename, error_counter):
    # Regular expression pattern for the specific address format
    pattern = r'^0x[a-fA-F0-9]{64}$'

    # Read the lines from the file
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Filter for valid Ethereum addresses
    valid_addresses = [line.strip() for line in lines if re.match(pattern, line.strip())]

    # Identify unique addresses
    unique_addresses = set(valid_addresses)

    # Calculate the number of duplicate addresses
    duplicates = len(valid_addresses) - len(unique_addresses)

    # Write the unique addresses back to the original file
    with open(filename, 'w', encoding='utf-8') as f:
        for address in unique_addresses:
            f.write(address + '\n')

    message = (f"**Report\n\n"
               f"*Time*: `{datetime.now().time()}`\n"
               f"*Totall addresses in DB*: `{len(unique_addresses)}`\n"
               f"*Duplicate deleted*: `{duplicates}`")
    await send_telegram_message(session, message, error_counter)



ua = UserAgent()

filename = 'addresses.txt'
progress_file = 'progress.txt'

try:
    with open(progress_file, 'r') as f:
        i = int(f.read().strip())
except FileNotFoundError:
    i = 275373795

pbar = tqdm(total=END - i, desc="Processing URLs | Addresses obtained: 0", ncols=100)

async def main_loop(i, end, fix, address_counter=0):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout, connector=aiohttp.TCPConnector(ssl=False)) as session:

        error_counter = 0

        while i < end:
            fix += 1
            batch_size = 1000
            results = await main(i, min(i + batch_size, end))

            with open(filename, 'a') as f:
                for first_address, second_address in results:
                    if first_address:
                        f.write(first_address + '\n')
                        address_counter += 1
                    if second_address:
                        f.write(second_address + '\n')
                        address_counter += 1

            # Update the progress bar description with the number of addresses obtained
            pbar.set_description(f"Processing URLs | Addresses obtained: {address_counter}")

            # Update progress in the progress file
            with open(progress_file, 'w') as f:
                f.write(str(i + batch_size))

            if fix == reports:
                fix = 0

                await clean_txt_file(session, filename, error_counter)
                # Send error report to Telegram
                if error_counter > 0:
                    message = f"{error_counter} requests failed after {MAX_RETRIES} retries in the last batch."
                    await send_telegram_message(session, message, error_counter)
                    error_counter = 0  # Reset the error counter for the next batch

            # Update tqdm progress bar
            pbar.update(batch_size)

            i += batch_size

# Now, run the main loop with these initial values
asyncio.run(main_loop(i, END, address_counter, fix))
pbar.close()
