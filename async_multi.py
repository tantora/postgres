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
DATA_SIZE = 1000


def randomname(n):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def create_items(n):
    return [{"{}".format(randomname(16)): random.random()} for x in range(n)]


async def async_zip(filename, d):
    print("{} zip start".format(filename))
    p = pickle.dumps(d)
    z = lz4.frame.compress(p)
    print("{} zip finish".format(filename))
    await async_write(filename, z)
    return filename


async def async_write(filename, z):
    # セマフォで同時のwrite数を制限制限しておく
    with await sem:
        print("{} write start".format(filename))
        # with open(filename, 'wb') as f:
        async with aiofiles.open(filename, 'wb') as f:
            await f.write(z)
        print("{} write finish".format(filename))
        return filename


async def async_create_data(filename):
    print("{} create start".format(filename))
    d = create_items(ITEM_NUM)
    await async_zip(filename, d)
    print("{} create finish".format(filename))
    return filename


# コルーチンだとpickleにできないって怒られるので関数にする
def zip_proc(*args):
    async def async_zip(filename, d, sem):
        print("{} zip start".format(filename))
        p = pickle.dumps(d)
        z = lz4.frame.compress(p)
        print("{} zip finish".format(filename))
        await async_write(filename, z, sem)
        return filename

    async def async_write(filename, z, sem):
        # セマフォで同時のwrite数を制限制限しておく
        with await sem:
            print("{} write start".format(filename))
            # with open(filename, 'wb') as f:
            async with aiofiles.open(filename, 'wb') as f:
                await f.write(z)
            print("{} write finish".format(filename))
            return filename

    async def async_create_data(filename):
        print("{} create start".format(filename))
        d = create_items(ITEM_NUM)
        await async_zip(filename, d)
        print("{} create finish".format(filename))
        return filename

    # 改めてイベントループを作る
    loop = asyncio.new_event_loop()
    try:
        # パラメータで受け取ったコルーチンをイベントループに登録
        sem = asyncio.Semaphore(2000, loop=loop)
        coro = async_zip(*args, sem)
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def async_create_cors(ds):
    cors = []
    i = 0
    # データセットのサイズ分タスクを作る

    for d in ds:
        filename = "{}/{}.lz4".format(DIR, str(i).zfill(16))
        cors.append(loop.run_in_executor(executor, zip_proc, filename, d))
        i += 1
    # タスクを実行
    done, pending = await asyncio.wait(cors)
    return done, pending



# データセットを作る
dataset = [create_items(ITEM_NUM) for x in range(DATA_SIZE)]

# ノンブロッキング用のループを作る
loop = asyncio.get_event_loop()

# multiprocess用のExecutorを用意
executor = concurrent.futures.ProcessPoolExecutor(max_workers=200)
loop.set_default_executor(executor)

start_time = datetime.datetime.now()

# ループで実行するコルーチンを指定
done, pending = loop.run_until_complete(async_create_cors(dataset))
# done, pending = loop.run_until_complete(async_create_cors())

finish_time = datetime.datetime.now()

print("start  : {}".format(start_time))
print("finish : {}".format(finish_time))

