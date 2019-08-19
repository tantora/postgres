import psycopg2
import string
import random
import time
import redis

def randomname(n):
    # return ''.join(random.choices(string.ascii_letters + string.digits, k=n))
    # return ''.join(random.choices(string.ascii_lowercase, k=n))
    li = ['0', '1', '2', '3', '4']
    return ''.join(random.choices(li, k=n))


def col2varchar(some_col):
    col_form = "'{}'::varchar"
    return col_form.format(some_col)


def col2char(some_col):
    col_form = "'{}'"
    return col_form.format(some_col)


def genre_sql_builder(num):
    genre_form = "select item from contents where ARRAY[{}] && genre"
    genres = ','.join(
        [col2varchar(randomname(2)) for i in range(num)])
    return genre_form.format(genres)


def item_sql_builder(num):
    item_form = "select item from contents where item = ANY (ARRAY[{}]);"
    items = ','.join(
        [col2varchar(str(random.randint(0, 9999999)).zfill(8)) for i in range(num)])
    return item_form.format(items)


def item_sql_byIN_builder(num):
    item_form = "select item from contents where item IN ({});"
    items = ','.join(
        [col2varchar(str(random.randint(0, 9999999)).zfill(8)) for i in range(num)])
    return item_form.format(items)


def item_sql_byINnotCast_builder(num):
    item_form = "select item from contents where item IN ({});"
    items = ','.join(
        [col2char(str(random.randint(0, 9999999)).zfill(8)) for i in range(num)])
    return item_form.format(items)


def gAi_sql_builder(gnum, inum):
    form = "select item from contents where item = ANY (ARRAY[{items}]) and ARRAY[{genres}] && genre;"
    genres = ','.join(
        [col2varchar(randomname(2)) for i in range(gnum)])
    items = ','.join(
        [col2varchar(str(random.randint(0, 9999999)).zfill(8)) for i in range(inum)])
    return form.format(items=items,genres=genres)

def gAi_sql_byINnotCast_builder(gnum, inum):
    form = "select item from contents where item IN ({items}) and ARRAY[{genres}] && genre;"
    genres = ','.join(
        [col2varchar(randomname(2)) for i in range(gnum)])
    items = ','.join(
        [col2char(str(random.randint(0, 9999999)).zfill(8)) for i in range(inum)])
    return form.format(items=items,genres=genres)


def gAi_sql_byJOIN_builder(gnum, inum):
    form = "select C.item from (select * from contents where genre && ARRAY[{genres}]) as G" \
               "  left join contents as C on G.item = C.item where C.item in ({items});"
    genres = ','.join(
        [col2varchar(randomname(2)) for i in range(gnum)])
    items = ','.join(
        [col2char(str(random.randint(0, 9999999)).zfill(8)) for i in range(inum)])
    return form.format(items=items, genres=genres)


def gAi_sql_outer_builder(gnum, inum):
    # form = "select C.item from (select item from genres where genre IN ({}) ) as G " \
    #        "left join contents as C on G.item = C.item where G.item in ({});"
    form = "select C.item from contents as C " \
           "where C.item in ({items}) and exists ( " \
           "select C.item from genres as G " \
           "where C.item = G.item and G.genre IN ({genres}) );"
    genres = ','.join(
        [col2char(randomname(2)) for i in range(gnum)])
    items = ','.join(
        [col2char(str(random.randint(0, 9999999)).zfill(8)) for i in range(inum)])
    return form.format(items=items, genres=genres)


def gAi_sql_app_builder(inum):
    form = "select item,genre,subgenre from contents where item IN ({items}) ;"
    items = ','.join(
        [col2char(str(random.randint(0, 9999999)).zfill(8)) for i in range(inum)])
    return form.format(items=items)


def genre_sql_executer(cur, gnum):
    cur.execute(genre_sql_builder(gnum))
    return(len(cur.fetchall()))


def item_sql_executer(cur, inum):
    cur.execute(item_sql_builder(inum))
    return(len(cur.fetchall()))


def item_sql_byIN_executer(cur, inum):
    cur.execute(item_sql_byIN_builder(inum))
    return(len(cur.fetchall()))


def item_sql_byINnotCast_executer(cur, inum):
    cur.execute(item_sql_byINnotCast_builder(inum))
    return(len(cur.fetchall()))


def gAi_sql_executer(cur, gnum, inum):
    cur.execute(gAi_sql_builder(gnum, inum))
    return(len(cur.fetchall()))


def gAi_sql_byINnotCast_executer(cur, gnum, inum):
    cur.execute(gAi_sql_byINnotCast_builder(gnum, inum))
    return(len(cur.fetchall()))


def gAi_sql_byJOIN_executer(cur, gnum, inum):
    cur.execute(gAi_sql_byJOIN_builder(gnum, inum))
    return(len(cur.fetchall()))


def gAi_sql_outer_executer(cur, gnum, inum):
    cur.execute(gAi_sql_outer_builder(gnum, inum))
    return(len(cur.fetchall()))


def gAi_sql_app_executer(cur, gnum, inum):
    genres = [randomname(2) for i in range(gnum)]
    cur.execute(gAi_sql_app_builder(inum))
    count = 0
    for row in cur:
        matched = set(row[1]) & set(genres)
        if len(matched) > 0:
            count += 1
    return count

def item_red_executer(redis, inum):
    val = redis.mget([str(random.randint(0, 9999999)).zfill(8) for i in range(inum)])
    return len(val)

def gAi_red_executer(redis, gnum, inum):
    genres = [randomname(2) for i in range(gnum)]
    vals = redis.mget([str(random.randint(0, 9999999)).zfill(8) for i in range(inum)])
    count = 0
    for val in vals:
        if isinstance(val, bytes):
            val = val.decode('utf-8')
        for v in val.split(","):
            for genre in genres:
                if genre == v:
                    count += 1
    return count


def speedgun(func, *args, **kargs):
    start = time.time()
    hit = func(*args, **kargs)
    process_time = time.time() - start
    return process_time, hit


def coaster(num, func, func2, *args, **kargs):
    result = [func(func2, *args, **kargs) for i in range(num)]
    form = "func:{} avg:{} max:{} min:{} num:{} avg_hit:{} sum_hit:{}"
    return form.format(
        func2.__name__,
        sum(x[0] for x in result) / num,
        max(x[0] for x in result),
        min(x[0] for x in result),
        num,
        int(sum(x[1] for x in result) / num),
        int(sum(x[1] for x in result)),
    )


url = "postgresql://hm3rd@127.0.01/postgres"
con = psycopg2.connect(url)
cur = con.cursor()
num = 1
inum = 4000
gnum = 20

print(genre_sql_builder(gnum))
print(item_sql_builder(inum))
print(item_sql_byIN_builder(inum))
print(item_sql_byINnotCast_builder(inum))
print(gAi_sql_builder(gnum, inum))
print(gAi_sql_byINnotCast_builder(gnum, inum))
print(gAi_sql_byJOIN_builder(gnum, inum))
print(gAi_sql_outer_builder(gnum, inum))
print(gAi_sql_app_builder(inum))

print(coaster(num, speedgun, item_sql_executer, cur, inum))
print(coaster(num, speedgun, item_sql_executer, cur, inum))
print(coaster(num, speedgun, item_sql_byIN_executer, cur, inum))
'''
print(coaster(num, speedgun, item_sql_byINnotCast_executer, cur, inum))
# print(coaster(num,speedgun,genre_sql_executer,cur,gnum))
print(coaster(num, speedgun, gAi_sql_executer, cur, gnum, inum))
print(coaster(num, speedgun, gAi_sql_byJOIN_executer, cur, gnum, inum))
print(coaster(num, speedgun, gAi_sql_byINnotCast_executer, cur, gnum, inum))
# print(coaster(num, speedgun, gAi_sql_outer_executer, cur, gnum, inum))
print(coaster(num, speedgun, gAi_sql_app_executer, cur, gnum, inum))
'''

cur.close()
con.close()

redis = redis.Redis(host='localhost', port=6379, db=0)
print(coaster(num, speedgun, item_red_executer, redis, inum))
print(coaster(num, speedgun, gAi_red_executer, redis, gnum, inum))

