import asyncio
import datetime
import os
import random
import string
import pickle
import lz4.frame
import aiofiles
import concurrent.futures

DIR = 'data'
os.makedirs(DIR, exist_ok=True)

ITEM_NUM = 1000
DATA_SIZE = 100000
WORKERS_NUM = 4

def randomname(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def create_items(n):
    return [{"{}".format(randomname(16)): random.random()} for x in range(n)]

def subproc_main(data_ids, data):
    async def async_zip(filename, d):
        print("{} zip start len:{}".format(filename, len(d)))
        p = pickle.dumps(d)
        z = lz4.frame.compress(p)
        print("{} zip finish".format(filename))
        await async_write(filename, z)
        return filename

    async def async_write(filename, z):
        # セマフォで同時のwrite数を制限制限しておく
        with await sem:
            print("{} write start len:{}".format(filename, len(z)))
            # with open(filename, 'wb') as f:
            async with aiofiles.open(filename, 'wb') as f:
                await f.write(z)
            print("{} write finish".format(filename))
            return filename

    async def subproc_async_main(data_ids):
        cors = []
        for di in data_ids:
            filename = "{}/{}.lz4".format(DIR, str(di).zfill(16))
            cors.append(async_zip(filename, data))
        await asyncio.wait(cors)

    loop = asyncio.new_event_loop()
    try:
        sem = asyncio.Semaphore(2000, loop=loop)
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(subproc_async_main(data_ids))
    finally:
        loop.close()

# データセットを作る
# dataset = [[x, create_items(ITEM_NUM)] for x in range(DATA_SIZE)]
dataset = range(DATA_SIZE)
data = create_items(ITEM_NUM)
sub_ds = [dataset[i::WORKERS_NUM] for i in range(WORKERS_NUM)]

# multiprocess用のExecutorを用意
executor = concurrent.futures.ProcessPoolExecutor(max_workers=WORKERS_NUM)

start_time = datetime.datetime.now()

futures = [executor.submit(subproc_main, sub_ds[i], data) for i in range(WORKERS_NUM)]

# 実行完了を待つ
for future in concurrent.futures.as_completed(futures):
    print(future.result())

finish_time = datetime.datetime.now()

print("start  : {}".format(start_time))
print("finish : {}".format(finish_time))

