import sys
import random
import string
import datetime
import numpy as np

'''
usage:
 python datagen.py 10000000 > data.tsv &
'''
args = sys.argv
counter = int(args[1]) if len(args) > 1 else 10


def randomname(n):
    li = ['0', '1', '2', '3', '4']
    seed = random.randint(0, 9)
    if seed < 2:
        return ''.join(random.choices(li, k=n))
    elif seed < 6:
        return ''.join(random.choices(string.ascii_lowercase, k=n))
    else:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=n))


def randomdate(now, null_weight, foward_weight, faraway):
    if random.randint(0, 9) < null_weight:
        return
    span = random.randint(0, faraway)
    if random.randint(0, 9) < foward_weight:
        return now + datetime.timedelta(hours=span)
    else:
        return now - datetime.timedelta(hours=span)


now = datetime.datetime.now();

for i in range(counter):
    item = str(i).zfill(8)
    # genre = ','.join([randomname(2) for i in range(random.randint(0,4))])
    genre = ','.join([randomname(2) for i in range(int(np.random.chisquare(df=2)))])
    subgenre = ','.join([randomname(2) for i in range(int(np.random.chisquare(df=1)))])
    start = randomdate(now, 2, 2, 48)
    start_date = start.strftime('%Y-%m-%d %H:%M:%S') if start is not None else ""
    stop = randomdate(now, 2, 8, 1000)
    stop_date = stop.strftime('%Y-%m-%d %H:%M:%S') if stop is not None else ""
    print("{}\t{{{}}}\t{{{}}}\t{}\t{}".format(item, genre, subgenre, start_date, stop_date))
