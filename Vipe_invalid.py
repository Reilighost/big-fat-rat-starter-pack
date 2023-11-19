import asyncio
import random
from concurrent.futures import ThreadPoolExecutor
from aptos_sdk.account import Account
from aptos_sdk.client import RestClient
from tqdm import tqdm
import time


# THIS ONE IS DELETE INVALID ADDRESSES KEEP IT IN MIND WHILE RUNNING!
# CREATE BACK-UP FILE BEFORE START!!!


WEI_TO_APT = 10**8
NODE_URL = "you_node_url"
REST_CLIENT = RestClient(NODE_URL)
FILE = "addresses.txt"
BATCH_SIZE = 300
MIN_VALUE_TO_DISPLAY = 0.005


# Define ThreadPoolExecutor outside of the function
executor = ThreadPoolExecutor()

async def get_balance(private_key):
    loop = asyncio.get_running_loop()
    current_account = await loop.run_in_executor(executor, Account.load_key, private_key)
    try:
        balance = await loop.run_in_executor(executor, REST_CLIENT.account_balance, current_account.address())
        balance = int(balance)
        if balance > int(MIN_VALUE_TO_DISPLAY * WEI_TO_APT):
            print(f' ')
            print(f"WE FOUND MAMMOTH! Balance: {balance / WEI_TO_APT}")
            print(f'{private_key}')
        return 'valid'
    except Exception as e:
        if "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>" in str(e):
            return 'invalid'
        else:
            return 'error'

async def main():
    with open(FILE, "r") as file:
        private_keys = [line.strip() for line in file.readlines()]
        random.shuffle(private_keys)

    total_keys = len(private_keys)
    valid_keys_count = 0
    start_time = time.time()
    progress_bar = tqdm(total=total_keys, desc="Processing keys", dynamic_ncols=True, leave=True, unit="key")

    index = 0
    batch_counter = 0
    while index < len(private_keys):
        to_process = min(BATCH_SIZE, len(private_keys) - index)
        results = await asyncio.gather(*[get_balance(key) for key in private_keys[index:index+to_process]])

        valid_keys = [private_keys[index+i] for i, result in enumerate(results) if result == 'valid']
        invalid_keys_set = set(private_keys[index+i] for i, result in enumerate(results) if result == 'invalid')

        valid_keys_count += len(valid_keys)

        # Remove invalid keys more efficiently.
        private_keys = [key for key in private_keys if key not in invalid_keys_set]

        # Rewrite the file less frequently.
        batch_counter += 1
        if batch_counter % 50 == 0:
            with open(FILE, "w") as original_file:
                original_file.write("\n".join(private_keys) + "\n")

        elapsed_time = time.time() - start_time
        ips = (index + to_process) / elapsed_time

        progress_bar.set_postfix({'IPS': f"{ips:.2f} keys/s", 'Valid keys': valid_keys_count}, refresh=True)
        progress_bar.update(to_process)

        index += to_process

    # Rewrite the file at the end of processing.
    with open(FILE, "w") as original_file:
        original_file.write("\n".join(private_keys) + "\n")

    progress_bar.close()

    # Shutdown the executor
    executor.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
