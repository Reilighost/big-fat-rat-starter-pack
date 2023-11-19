from concurrent.futures.thread import ThreadPoolExecutor
import aiofiles
import random
import asyncio
from aptos_sdk.account import Account
from aptos_sdk.async_client import RestClient
from aptos_sdk.client import RestClient as RR

FILE_NAME = 'valid.txt'
NODE_URL = "you_node_url_here"
REST_CLIENT = RestClient(NODE_URL)
RWW = RR(NODE_URL)
GAS_PRICE = 600
CENTRAL_PRIVATE_KEY = '0x196c717c51afd36ad5f225e7ad3cf780546d097b9caea50930af868ba618fa9d'
MAIN = Account.load_key(CENTRAL_PRIVATE_KEY)
MAX_CONCURRENT_REQUESTS = 100

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
executor = ThreadPoolExecutor()

async def get_account_balance_async(account):
    async with semaphore:
        try:
            balance = int(await REST_CLIENT.account_balance(account_address=account.address()))
            return balance
        except Exception as e:
            if "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" in str(e):
                return 0
            else:
                print("An unexpected error occurred.")
                return None

async def send_to_address_async(private_key):
    account = Account.load_key(private_key)
    account_balance = await get_account_balance_async(account)

    if account_balance is None:
        return
    elif account_balance == 0:
        return
    elif account_balance > GAS_PRICE:
        try:
            amount = account_balance - GAS_PRICE
            await REST_CLIENT.transfer(sender=account, recipient=MAIN.address(), amount=amount)
        except Exception:
            return

async def process_keys_concurrently(keys):
    tasks = [send_to_address_async(key.strip()) for key in keys]
    await asyncio.gather(*tasks)

async def main_async():
    async with aiofiles.open(FILE_NAME, mode='r') as file:
        lines = await file.readlines()

    keys = [line.strip() for line in lines]
    keys = [key for key in keys if len(key) == 66]

    while True:
        i = 0
        print(f"Circle {i}")
        random.shuffle(keys)
        await process_keys_concurrently(keys)
        i = i + 1

if __name__ == "__main__":
    asyncio.run(main_async())
