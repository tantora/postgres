import redis
import psycopg2
import pickle
from tqdm import tqdm

redis = redis.Redis(host='localhost', port=6379, db=0)
pipe = redis.pipeline()

url = "postgresql://hm3rd@127.0.01/postgres"
con = psycopg2.connect(url)
cur = con.cursor('cursol')
sql = "select * from contents ;"
cur.execute(sql)
# for row in tqdm(cur):
size = 500000
rows = cur.fetchmany(size)
stored = 0
i = 0

while len(rows) > 0:
    for row in rows:
        a = {"item": row[0], "genre": row[1], "subgenre": row[2], "start": row[3], "stop": row[4]}
        dump = pickle.dumps(a)
        pipe.set(row[0], dump)
        i += 1

    resp = pipe.execute()
    stored += resp.count(True)
    print("set to redis... selected row: {}, success: {}".format(i, stored))
    rows = cur.fetchmany(size)

print("finish.")
cur.close()
con.close()
# load = redis.get("dump")
# x = pickle.loads(load)
# print(x)
# print(x['item'])
# print(x['genre'])
