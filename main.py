# Import necessary libraries
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns

def load_data():
    """Load the Iris dataset"""
    data = load_iris()
    df = pd.DataFrame(data=data.data, columns=data.feature_names)
    return df

def preprocess_data(df):
    """Preprocess the data by standardizing it"""
    scaler = StandardScaler()
    scaled_df = scaler.fit_transform(df)
    return pd.DataFrame(scaled_df, columns=df.columns)

def apply_kmeans(df, num_clusters=3):
    """Apply K-means clustering"""
    kmeans = KMeans(n_clusters=num_clusters, n_init=10, random_state=42)
    labels = kmeans.fit_predict(df)
    df['Cluster'] = labels
    return df, kmeans

def visualize_clusters(df):
    """Visualize the clustered data using the first two features and save as an image."""
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=df[df.columns[0]], y=df[df.columns[1]], hue=df['Cluster'], palette='viridis')
    plt.title('Cluster Visualization')
    plt.xlabel(df.columns[0])
    plt.ylabel(df.columns[1])
    plt.legend(title='Cluster')
    plt.savefig('/app/cluster_visualization.png')  # Save the plot to a file
    plt.close()  # Close the plot window to free up resources

def main():
    df = load_data()
    processed_df = preprocess_data(df)
    clustered_df, kmeans = apply_kmeans(processed_df)
    visualize_clusters(clustered_df)

if __name__ == "__main__":
    main()
