from flask import Flask, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

app = Flask(__name__)

limiter = Limiter(
    get_remote_address,
    app=app
)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api")
@limiter.limit("3/second")
def api():
    return "Hello World!"

if __name__ == "__main__":
    app.run(debug=True)
