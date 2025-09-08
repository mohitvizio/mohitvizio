# Databricks notebook source
!pip install yellowbrick

# COMMAND ----------

!pip install --upgrade threadpoolctl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
from yellowbrick.cluster import KElbowVisualizer,SilhouetteVisualizer
import matplotlib.pyplot as plt

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

from sklearn.preprocessing import StandardScaler

from pyspark.sql import functions as F

%matplotlib inline

plt.logging.getLogger('matplotlib.font_manager').disabled = True


# COMMAND ----------

df = spark.sql("select * from dev.mohit_gangwani.wfp_persona_dataset TABLESAMPLE (20 PERCENT) where viewing_hours >= 1")

# COMMAND ----------

v1_features = [
    "all_unique_channels",
    "all_unique_fast_channels",
    "all_unique_ota_channels",
    "all_unique_categories",
    # "last_session",
    # "last_3m_session",
    # "num_sessions",
    "num_3min_sessions",
    "viewing_hours",
    "fast_hours",
    "ota_hours",
    # "avg_distinct_channels_per_session",
    # "avg_distinct_fast_channels_per_session",
    # "avg_distinct_ota_channels_per_session",
    # "avg_distinct_categories_per_session",
    "num_days_active",
    # 'average_session_duration',
]

df_subset_pd = df.toPandas().fillna(0)

df_subset_pd = df_subset_pd[v1_features]

df_subset_pd.loc[:, 'average_session_duration'] = df_subset_pd['viewing_hours'] / df_subset_pd['num_3min_sessions']

# COMMAND ----------

def gini_index(values):
    """Calculate the Gini index for a given distribution of values."""
    probs = np.bincount(values) / len(values)
    return 1 - np.sum(probs ** 2)

# COMMAND ----------

def mean_median_ratio(values):
    """Calculate the Mean-to-Median Ratio (MMR) for skewness analysis."""
    mean_val = np.mean(values)
    median_val = np.median(values)
    return mean_val / median_val if median_val != 0 else 0

# COMMAND ----------

def overall_metric(df, labels, metric='gini'):
    """Calculate the weighted metric (Gini or MMR) for the dataset."""
    total_metric = 0
    for cluster in np.unique(labels):
        cluster_data = df[labels == cluster]
        if metric == 'gini':
            cluster_metric = np.mean([gini_index(cluster_data[col].values) for col in df.columns])
        else:
            cluster_metric = np.mean([mean_median_ratio(cluster_data[col].values) for col in df.columns])
        total_metric += cluster_metric * (len(cluster_data) / len(df))
    return total_metric

# COMMAND ----------

def cluster_with_metric_reduction(df, n_clusters=3, max_iter=100, metric='gini'):
    """Cluster the DataFrame while minimizing the Mean-to-Median Ratio or Gini index."""
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df)
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=25)
    labels = kmeans.fit_predict(df_scaled)
    
    best_metric = overall_metric(df, labels, metric)
    best_labels = labels.copy()
    
    for _ in range(max_iter):
        kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=25)
        labels = kmeans.fit_predict(df_scaled)
        current_metric = overall_metric(df, labels, metric)
        
        if current_metric < best_metric:
            best_metric = current_metric
            best_labels = labels.copy()
    
    return best_labels, best_metric, kmeans

# COMMAND ----------

def find_best_k(df, min_k=2, max_k=10, x_init=10):
    """Determine the best number of clusters using the Elbow Method and Silhouette Score."""
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df)
    
    inertia = []
    silhouette_scores = []
    
    for k in range(min_k, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=x_init)
        labels = kmeans.fit_predict(df_scaled)
        inertia.append(kmeans.inertia_)
        silhouette_scores.append(silhouette_score(df_scaled, labels))
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax2 = ax1.twinx()
    ax1.plot(range(min_k, max_k + 1), inertia, 'bo-', label='Inertia')
    ax2.plot(range(min_k, max_k + 1), silhouette_scores, 'ro-', label='Silhouette Score')
    
    ax1.set_xlabel('Number of Clusters')
    ax1.set_ylabel('Inertia', color='b')
    ax2.set_ylabel('Silhouette Score', color='r')
    plt.title('Elbow Method and Silhouette Score for Optimal K')
    plt.show()

# COMMAND ----------

def visualize_clusters(df, labels):
    """Visualize the clustering results using PCA for dimensionality reduction."""
    from sklearn.decomposition import PCA
    pca = PCA(n_components=2)
    df_pca = pca.fit_transform(StandardScaler().fit_transform(df))
    
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=df_pca[:, 0], y=df_pca[:, 1], hue=labels, palette='viridis', alpha=0.7, sizes=(10, 10))
    plt.title('Clusters Visualization (PCA Projection)')
    plt.xlabel('Principal Component 1')
    plt.ylabel('Principal Component 2')
    plt.legend(title='Cluster')
    plt.show()

# COMMAND ----------

df_subset_pd.columns

# COMMAND ----------

# v2_features = [
#     "all_unique_channels",
#     # "all_unique_fast_channels",
#     "all_unique_ota_channels",
#     "all_unique_categories",
#     # "CRIME_hours",
#     # "FEATURED_hours",
#     # "LATINO_hours",
#     # "MOVIES_hours",
#     # "NEWS_OPINION_hours",
#     # "local_content_hours",
#     "num_3min_sessions",
#     "viewing_hours",
#     "fast_hours",
#     "ota_hours",
#     "avg_distinct_channels_per_session",
#     "avg_distinct_fast_channels_per_session",
#     "avg_distinct_ota_channels_per_session",
#     "avg_distinct_categories_per_session",
#     "num_days_active",
# ]

# df_subset_pd = df_subset_pd[v2_features]

# COMMAND ----------

df_subset_pd.corr().display()

# COMMAND ----------

# sns.set_style("darkgrid")

# numerical_columns = df_subset_pd.select_dtypes(include=["int64", "float64"]).columns

# plt.figure(figsize=(14, len(numerical_columns) * 3))
# for idx, feature in enumerate(numerical_columns, 1):
#     plt.subplot(len(numerical_columns), 2, idx)
#     sns.histplot(df_subset_pd[feature], kde=True)

# plt.tight_layout()
# plt.show()

# COMMAND ----------

stat_df = pd.DataFrame(
    columns=["feature", "non_zero_count", "mean", "median", "variance",
             "standard_deviation", "range", "IQR", "skewness", "kurtosis"]
    )
for col in df_subset_pd.columns:
    stat_df.loc[col, "feature"] = col
    stat_df.loc[col, 'non_zero_count'] = np.count_nonzero(df_subset_pd[col])
    stat_df.loc[col, 'mean'] = round(df_subset_pd[col].mean(), 2)
    stat_df.loc[col, 'variance']  = round(df_subset_pd[col].var(), 2)
    stat_df.loc[col, 'standard_deviation']  = round(df_subset_pd[col].std(), 2)
    stat_df.loc[col, 'range']  = round(df_subset_pd[col].max() - df_subset_pd[col].min(), 2)
    stat_df.loc[col, 'IQR']  = round(df_subset_pd[col].quantile(0.75) - df_subset_pd[col].quantile(0.25), 2)
    stat_df.loc[col, 'median']  = round(df_subset_pd[col].median(), 2)
    stat_df.loc[col, 'skewness']  = round(df_subset_pd[col].skew(), 2)
    stat_df.loc[col, 'kurtosis']  = round(df_subset_pd[col].kurt(), 2)

stat_df.display()

# COMMAND ----------

plt.figure(figsize=(15, 10))

sns.heatmap(df_subset_pd.corr(), annot=True, cmap="crest", fmt='.2f', linewidths=2)

plt.title('Correlation Heatmap')
plt.show()

# COMMAND ----------

df_subset_pd.info()

# COMMAND ----------

km = KMeans(random_state=42)
visualizer = KElbowVisualizer(km, k=(3,9), metric='silhouette')

visualizer.fit(df_subset_pd)        # Fit the data to the visualizer
visualizer.show()

# COMMAND ----------

print(visualizer.k_scores_)
print(visualizer.k_timers_)
print(visualizer.elbow_value_)
print(visualizer.elbow_score_)

# COMMAND ----------

find_best_k(df_subset_pd, min_k=3, max_k=9, x_init=25)

# COMMAND ----------

labels, final_metric, kmeans = cluster_with_metric_reduction(df_subset_pd, n_clusters=4, metric='mean_median')

# COMMAND ----------

visualize_clusters(df_subset_pd, labels)

# COMMAND ----------

print("Final Metric Value:", final_metric)

# COMMAND ----------

kmeans.cluster_centers_

# COMMAND ----------

df_subset_pd.loc[:, 'cluster'] = labels

# COMMAND ----------

df_subset_pd.groupby("cluster").mean()

# COMMAND ----------

df_subset_pd.groupby("cluster").median()

# COMMAND ----------

spark_df = spark.createDataFrame(df_subset_pd)

# COMMAND ----------

spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("dev.mohit_gangwani.wfp_persona_with_labels_new")

# COMMAND ----------

for i in range(6):
    print(f"Cluster {i}: {len(df_subset_pd[df_subset_pd['cluster'] == i])}")

# COMMAND ----------

def gini(x):
    probs = np.bincount(x) / len(x)
    return 1 - np.sum(probs ** 2)

def mmr(x):
    return np.mean(x)/np.median(x) if np.median(x) != 0 else 0

# COMMAND ----------

gini_df = pd.DataFrame()
for i in range(3, -1, -1):
    for col in df_subset_pd.columns[:-1]:
        gini_df.loc[i, 'cluster'] = f'cluster {i}'
        gini_df.loc[i, col] = gini(df_subset_pd[df_subset_pd['cluster'] == i][col])
        print(f'Cluster {i}, metric: {col} completed')
        # print(col)
        # print(f"Cluster {i} Gini: {gini(df_subset_pd[df_subset_pd['cluster'] == i][col])}")

# COMMAND ----------

mmr_df = pd.DataFrame()
for i in range(3, -1, -1):
    for col in df_subset_pd.columns[:-1]:
        mmr_df.loc[i, 'cluster'] = f'cluster {i}'
        mmr_df.loc[i, col] = mmr(df_subset_pd[df_subset_pd['cluster'] == i][col])
        print(f'Cluster {i}, metric: {col} completed')

# COMMAND ----------

mmr_df.display()

# COMMAND ----------

gini_df.display()

# COMMAND ----------

clust_stat_df = pd.DataFrame(
    columns=["feature", "cluster", "non_zero_count", "mean", "median", "variance",
             "standard_deviation", "range", "IQR", "skewness", "kurtosis"]
    )

for clust in range(4):
    clust_df = pd.DataFrame()
    ndf = df_subset_pd[df_subset_pd['cluster'] == clust]
    for col in df_subset_pd.columns[:-1]:
        clust_df.loc[col, "feature"] = col
        clust_df.loc[col, "cluster"] = clust
        clust_df.loc[col, 'non_zero_count'] = np.count_nonzero(ndf[col])
        clust_df.loc[col, 'mean'] = round(ndf[col].mean(), 2)
        clust_df.loc[col, 'variance']  = round(ndf[col].var(), 2)
        clust_df.loc[col, 'standard_deviation']  = round(ndf[col].std(), 2)
        clust_df.loc[col, 'range']  = round(ndf[col].max() - ndf[col].min(), 2)
        clust_df.loc[col, 'IQR']  = round(ndf[col].quantile(0.75) - ndf[col].quantile(0.25), 2)
        clust_df.loc[col, 'median']  = round(ndf[col].median(), 2)
        clust_df.loc[col, 'skewness']  = round(ndf[col].skew(), 2)
        clust_df.loc[col, 'kurtosis']  = round(ndf[col].kurt(), 2)
    clust_stat_df = pd.concat([clust_stat_df, clust_df], ignore_index=True)

clust_stat_df.display()

# COMMAND ----------

fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))

centroids = kmeans.cluster_centers_
plotted_combinations = set()  # Use a set for efficient look-up
for i, feature_i in enumerate(v1_features):
    for j, feature_j in enumerate(v1_features[i + 1:], start = i + 1):  # Avoid repeating combinations
        combination_key = f"{feature_i}+{feature_j}"
        if combination_key not in plotted_combinations:
            plotted_combinations.add(combination_key)
            plt.scatter(
                df_subset_pd[feature_i],
                df_subset_pd[feature_j],
                c=df_subset_pd["cluster"],
                cmap="brg",
            )
            for k in range(4):  # Assuming there are 4 centroids
                plt.scatter(
                    centroids[k, i], centroids[k, j], s=200, c="black"
                )  # Adjust indexing for centroids

# Consider saving the figure instead of displaying it directly if running out of memory
# plt.savefig("scatter_plot_matrix.png")
plt.show()  # Uncomment if you want to display the plot inline (may consume a lot of memory)

# COMMAND ----------

fig, ax = plt.subplots(3, 2, figsize=(15, 8))
for i in [2, 3, 4]:
    """
    Create KMeans instances for different number of clusters
    """
    km = KMeans(
        n_clusters=i, init="k-means++", n_init=10, max_iter=100, random_state=42
    )
    q, mod = divmod(i, 2)
    """
    Create SilhouetteVisualizer instance with KMeans instance
    Fit the visualizer
    """
    visualizer = SilhouetteVisualizer(km, colors="yellowbrick", ax=ax[q - 1][mod])
    visualizer.fit(df_subset_pd.fillna(0))

# COMMAND ----------


