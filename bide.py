from prefixspan import PrefixSpan
import psycopg2

db = []

conn = psycopg2.connect(host="localhost", port = 5432, database="state", user="postgres", password="root")
cur = conn.cursor()
cur.execute("""select b.sequence from (select a.page_session as id, array_agg(a.action || ' ' || a.target order by a.time) as sequence from action a group by a.page_session) b""")
for row in cur:
    sequence = row[0]
    db.append(sequence)
cur.close()
conn.close()

# db = [
#     [0, 1, 2, 3, 4],
#     [1, 1, 1, 3, 4],
#     [2, 1, 2, 2, 0],
#     [1, 1, 1, 2, 2],
# ]

ps = PrefixSpan(db)

print(db)

print ('\n')

print(ps.frequent(2, closed=True))

print('\n')

print(ps.topk(10, closed=True))