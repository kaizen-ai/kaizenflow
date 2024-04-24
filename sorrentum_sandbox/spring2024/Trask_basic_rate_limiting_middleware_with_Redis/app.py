from flask import Flask
import redis
from datetime import timedelta
from flask import request

r = redis.Redis(host='redis', port=6379, db=0)

def request_is_limited(r: redis.Redis, key: str, limit: int, period: timedelta):
    period_in_seconds = int(period.total_seconds())
    t = r.time()[0]
    separation = round(period_in_seconds / limit)
    r.setnx(key, 0)
    try:
        with r.lock('lock:' + key, blocking_timeout=5) as lock:
            tat = max(int(r.get(key)), t)
            if tat - t <= period_in_seconds - separation:
                new_tat = max(tat, t) + separation
                r.set(key, new_tat)
                return False
            return True
    except LockError:
        return True
app = Flask(__name__)

@app.route('/')
def hello():
   pings = int(request.args.get("pings", 1))
   block_flag = False
   for i in range(pings):
      if request_is_limited(r, "admin", 10, timedelta(minutes=1)):
         block_flag = True
   if block_flag:
      return '<h1>BLOCKED </h2>'
   else:
      return (
		"""<form action="" method="get">
		<input type="text" name="pings">
		<input type="submit" value="Convert">
		</form>""")

if __name__ == "__main__":
    app.run(debug=True)
