import asyncio
import datetime
import os
import random
import string
import pickle
import lz4.frame
import aiofiles

DIR = 'data'
os.makedirs(DIR, exist_ok=True)

ITEM_NUM = 5000
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
    print("{} write start".format(filename))
    # with open(filename, 'wb') as f:
    async with aiofiles.open(filename, 'wb') as f:
        f.write(z)
    print("{} write finish".format(filename))
    return filename


async def async_create_data(filename):
    print("{} create start".format(filename))
    d = create_items(ITEM_NUM)
    await async_zip(filename, d)
    print("{} create finish".format(filename))
    return filename


# async def async_create_cors(ds):
async def async_create_cors():
    cors = []
    i = 0
    # for d in ds:
    for i in range(DATA_SIZE):
        filename = "{}/{}.lz4".format(DIR, str(i).zfill(16))
        cors.append(async_create_data(filename))
        # cors.append(async_zip(filename,d))
        # i += 2
    done, pending = await asyncio.wait(cors)
    return done, pending


# dataset = [create_items(ITEM_NUM) for x in range(DATA_SIZE)]

print("create dataset end: {}".format(datetime.datetime.now()))
loop = asyncio.get_event_loop()
# done, pending = loop.run_until_complete(async_create_cors(dataset))
done, pending = loop.run_until_complete(async_create_cors())

for d in done:
    dr = d.result()
    # print(dr)
