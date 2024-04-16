from flask import Flask, render_template, jsonify
import json

app = Flask(__name__)

with open('static/data.json', encoding='UTF-8') as file:
    data = json.load(file)

@app.route('/')
def home():
    return render_template('index.html', data=data)

@app.route('/api/<int:i>')
def api(i):
    return jsonify(data[i])

if __name__ == '__main__':
    app.run(debug=True)
