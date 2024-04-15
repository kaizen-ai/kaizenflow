from flask import Flask
from redis import Redis

app = Flask(__name__)
redis = Redis(host='redis', port=6379)

@app.route('/')
def hello():
    # Store a value in Redis
    redis.set('mykey', 'Hello, Redis!')
    
    # Retrieve the value from Redis
    return redis.get('mykey').decode('utf-8')

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
