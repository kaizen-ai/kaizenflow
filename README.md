# ZooKeeper Integration with Data Mining
ZooKeeper Integration with Data Mining

## Overview
This project integrates ZooKeeper with data mining tasks using Pandas for data analysis. It demonstrates fetching data from ZooKeeper nodes, converting it into Pandas DataFrames, and visualizing results using matplotlib/seaborn libraries. 

## Technologies Used
- ZooKeeper
- Pandas
- Docker
- Docker Compose
- Redis
- Jupyter Notebook
- Git/GitHub

## Setup and Installation

### Prerequisites
- Docker
- Docker Compose
- Python 3.x
- Git

### Installation Steps
1. **Clone the Repository**:

git clone https://github.com/kaizen-ai/kaizenflow.git
cd kaizenflow

2. **Install Dependencies**:
- For Python packages:
  ```
  pip install -r requirements.txt
  ```
- For Docker:
  ```
  docker-compose up --build
  ```

Here's a structured guide to set up and run your project involving ZooKeeper, Pandas, Redis, Jupyter Notebook, and GitHub:



### Clone the Repository
```bash
git clone https://github.com/kaizen-ai/kaizenflow.git
cd kaizenflow
```

### Setup Environment
- Install Python dependencies:
  ```bash
  pip install -r requirements.txt
  ```
- Start Redis and ZooKeeper using Docker:
  ```bash
  docker-compose up -d redis zookeeper
  ```

### Running Jupyter Notebook
- Launch Jupyter Notebook to access and run your Python notebooks:
  ```bash
  jupyter notebook
  ```

### Utilize ZooKeeper and Redis
- Connect to ZooKeeper within your Python code to manage configurations or state.
- Use Redis for caching data to enhance performance.
### Data
- Cryptocurrencies Including:
  - Bitcoin
  - Ethereum
  - Solana
  - Injective
  - Render
  - Cardano
  - Chainlink
  - Polygon
  - Decentrailized
  - Axie Infinity
  - Enjin Coin
  
Apparently the free version of kraken provides data from the beginnign of the year 2022  

### Data Analysis with Pandas
- Load the data for 11 specified cryptos into Pandas DataFrames for the following analysis and visualization in Jupyter Notebook.
  - Summary of statistics
  - Diagram of Closing Prices Over Time 
  - Histogram of Daily Percentage Change
  - Correlation Matrix of Closing Prices
  - Moving Averages
  - Relative Strength Index (RSI)
  - Bollinger Bands
  - MACD Histogram
  - Trend Analysis
  - 
### Version Control
- Use Git to manage and version your project code. Push updates to GitHub:
  ```bash
  git add .
  git commit -m "Updated analysis"
  git push origin main
  ```

3. **Environment Setup**:
- Copy the `GITHUB_PAT=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` file to `.env` and modify it according to your local environment settings.

4. **Running the Application**:
- To start the application:
  ```
  docker-compose up
  ```
- Access the application via `localhost:PORT` where `PORT` is the port specified in your Docker or environment settings.

### Additional Configuration


### Installation Steps
1. **Clone the Repository**:

## Contribution Guidelines

## License
This project has been for educational purposes.

## Contact
For more information, contact [Farhad Abasahl](mailto:farhad@umd.edu).
