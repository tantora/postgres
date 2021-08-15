from concurrent.futures import ThreadPoolExecutor
import random
import time
import numpy as np
import pathlib
import string
import csv
import io

print(pathlib.Path.cwd())
DIR = pathlib.Path.cwd().parent.joinpath('data4')
DIR.mkdir(exist_ok=True)

UNUM = 7000
CNUM = 1000
LIMIT = 1000
BSIZE = 10
LOAD = 1
WORKER_NUM = 100


random.seed(a=12)
np.random.seed(12)


def randstr(n):
    chars = string.ascii_lowercase + string.ascii_uppercase + string.digits
    return ''.join([random.choice(chars) for i in range(n)])


def fizz_buzz(n):
    l = []
    for i in range(1, n + 1):
        r = ''
        if i % 3 == 0:
            r += 'fizz'
        if i % 5 == 0:
            r += 'buzz'
        if not r:
            r = str(i)
        l.append(r)
    return l


def create_score(uids, cids):
    fizz_buzz(len(uids) * LOAD)
    scores = np.random.rand(len(uids) * len(cids))
    return scores


def write_file(uid, rect, dstdir):
    start = time.time()
    wpath = dstdir.joinpath('{}.tsv'.format(uid))
    print('write start {}'.format(wpath))
    with io.StringIO() as f:
        writer = csv.writer(f, delimiter='\t', lineterminator='\n')
        writer.writerows(rect)
        with open(wpath, 'w') as w:
            w.write(f.getvalue())
            #await asyncio.sleep(0.01)
            #print('write finish {}'.format(wpath))
            stop = time.time()
            print('write finish: {:.3f} sec, file: {}'.format(stop - start, wpath))


def subbatch(uids, cids, wdir, limit):
    unum = len(uids)
    cnum = len(cids)
    start = time.time()
    #print('calc start {}'.format(wdir))
    scores = create_score(uids, cids)
    stop = time.time()
    #print('calc finish {}'.format(wdir))
    print('calc time: {:.3f}, dir: {}'.format(stop - start, wdir))
    scores = scores.reshape([unum, cnum])
    #print('scores:{}'.format(scores[0:1]))

    srtscores = np.sort(scores, axis=1)[:, ::-1][:, :limit]
    srtindices = np.argsort(scores, axis=1)[:, ::-1][:, :limit]
    #print('srtscores:{}'.format(srtscores[0:1]))
    #print('srtindices:{}'.format(srtindices[0:1]))

    for uidx, uid in enumerate(uids):
        rect = [(cids[cidx], score) for cidx, score in zip(srtindices[uidx], srtscores[uidx])]
        #print('uidx:{}, uid:{}, reco:{}'.format(uidx, uid, rect[:2]))

        write_file(uid, rect, wdir)


def batch(uids, cids, bsize, limit, dir):
#def batch(uids, cids, bsize, limit, dir):
    unum = len(uids)
    loop_num = -(-unum // bsize)
    executor = ThreadPoolExecutor(max_workers=WORKER_NUM, thread_name_prefix='subbatch')

    futures = []
    for i in range(loop_num):
        start = i * bsize
        stop = (i + 1) * bsize
        divuids = uids[start:stop]
        divunum = len(divuids)
        #print('loop:{}/{}, start:{}, stop:{}, lunum:{}'.format(i + 1, loop_num, start, stop, divunum))
        #print('uids:{}'.format(divuids[0:4]))

        wdir = dir.joinpath('{}'.format(i))
        wdir.mkdir(exist_ok=True)

        #subbatch(divuids, cids, wdir, limit)
        #task = loop.create_task(subbatch(divuids, cids, wdir, limit))
        futures.append(executor.submit(subbatch, divuids, cids, wdir, limit))

    result = [f.result() for f in futures]
    return result
    #return tasks


def main():
    uids = [randstr(8) for i in range(UNUM)]
    cids = [randstr(8) for i in range(CNUM)]
    start = time.time()
    batch(uids, cids, BSIZE, LIMIT, DIR)
    stop = time.time()
    print(' ---- \nprocessing: {:.3f} sec'.format(stop - start))


if __name__ == '__main__':
    main()
