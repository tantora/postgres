import asyncio
from concurrent.futures import ThreadPoolExecutor

async def mysleep2(*args):
    print("mysleep2 start {} {}".format(*args))
    await asyncio.sleep(*args)
    print("mysleep2 finish  {} {}".format(*args))

async def mysleep(*args):
    print("mysleep  start {} {}".format(*args))
    await asyncio.sleep(*args)
    await mysleep2(args[0]*2, args[1])
    print("mysleep  finish {} {}".format(*args))

def run(corofn, *args):
    loop = asyncio.new_event_loop()
    try:
        coro = corofn(*args)
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        loop.close()


async def main():
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=200)
    futures = [
        loop.run_in_executor(executor, run, mysleep, 1, x)
        for x in range(10)]
    print(await asyncio.gather(*futures))
    # Prints: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())