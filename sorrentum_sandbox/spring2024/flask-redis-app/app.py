from flask import Flask, render_template
from redis import Redis
import pandas as pd
import matplotlib.pyplot as plt
import io
import base64
import csv
import numpy as np
import seaborn as sns

app = Flask(__name__)
redis = Redis(host='redis', port=6379)

def load_data_from_csv(filename):
    data = []
    with open(filename, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            data.append(row)
    return data

@app.route('/')
def index():
    # Load data from CSV file
    data = load_data_from_csv('users_updated.csv')


    #  Store data in Redis
    for user in data:
        key = "user:{}".format(user['id'])
        redis.hmset(key, user)
    
    #    # Count frequency of each country
    country_counts = {}
    for user in data:
        country = user['country']
        if country in country_counts:
            country_counts[country] += 1
        else:
            country_counts[country] = 1

    # Prepare data for plotting
    countries = list(country_counts.keys())
    counts = list(country_counts.values())

    # Plot visualization (pie chart of countries)
    plt.figure(figsize=(8, 6))
    plt.pie(counts, labels=countries, startangle=140, colors=sns.color_palette('pastel'))
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title('Distribution of Users by Country')

    # Convert plot to base64 encoding
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)
    plot_url = base64.b64encode(img.getvalue()).decode()

    # Render template with plot
    return render_template('index.html', plot_url=plot_url)



if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
