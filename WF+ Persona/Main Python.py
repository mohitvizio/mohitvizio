# Databricks notebook source
# start_date = "'2024-12-01'"
# end_date = "'2024-12-31'"

# wfp_usage_sql = f'''
# with by_token_session as (
#        select token, 
#               sctv_session_id,
#               min(session_start) as session_start, 
#               datediff(second, min(session_start),max(session_end)) as app_seconds, 
#               sum(session_duration) as viewing_seconds, 
#               sum(case when sctv_channel_category[0] not in ('ANTENNA') then session_duration end) as fast_seconds, 
#               sum(case when sctv_channel_category[0] in ('ANTENNA')  then session_duration end) as ota_seconds, 
#               count(distinct airings_key) as distinct_channels,
#               count(distinct case when sctv_channel_category[0] not in ('ANTENNA', 'LOCAL CHANNELS')  then airings_key end) as distinct_FAST_channels,
#               count(distinct case when sctv_channel_category[0] in ('ANTENNA')  then airings_key end) as distinct_ota_channels,
#               count(distinct case when sctv_channel_category[0] in ('LOCAL CHANNELS')  then airings_key end) as distinct_ott_channels,
#               count(distinct sctv_channel_category[0]) as distinct_categories
#        from datalake_shares.agg.agg_wfp_live_channel_sessions_v1
#        where (date(session_start) between {start_date} and {end_date})
#        and session_duration >= 180
#        group by 1,2)
# select token, 
# count(distinct sctv_session_id) as num_sessions,
# count(distinct case when app_seconds >= 180 then sctv_session_id end) as num_3min_sessions, 
# sum(viewing_seconds)/(60*60)  as viewing_hours,
# sum(fast_seconds)/(60*60)  as fast_hours,
# sum(ota_seconds)/(60*60)  as ota_hours,
# avg(distinct_channels) as avg_distinct_channels_per_session,
# avg(distinct_FAST_channels) as avg_distinct_fast_channels_per_session,
# avg(distinct_ota_channels) as avg_distinct_ota_channels_per_session,
# avg(distinct_categories) as avg_distinct_categories_per_session,
# count(distinct session_start::date) as num_days_active
# from by_token_session
# group by 1
# '''

# wfp_usage = spark.sql(wfp_usage_sql)

# COMMAND ----------

# content_prefernece_sql = f'''
# select token, 
#        count(distinct airings_key) all_unique_channels,  
#        count(distinct case when sctv_channel_category[0] != 'ANTENNA' then airings_key end) all_unique_fast_channels,
#        count(distinct case when sctv_channel_category[0] = 'ANTENNA' then airings_key end) all_unique_ota_channels, 
#        count(distinct sctv_channel_category[0]) all_unique_categories,
#        sum(case when sctv_channel_category[0] in ('COMEDY') then session_duration end) as comedy_hours, 
#        sum(case when sctv_channel_category[0] in ('CRIME') then session_duration end) as CRIME_hours, 
#        sum(case when sctv_channel_category[0] in ('CULTURE + LIFESTYLE', 'INTERESTS + LIFESTYLE') then session_duration end) as lifestyle_hours, 
#        sum(case when sctv_channel_category[0] in ('DISCOVER') then session_duration end) as DISCOVER_hours, 
#        sum(case when sctv_channel_category[0] in ('ENTERTAINMENT') then session_duration end) as ENTERTAINMENT_hours, 
#        sum(case when sctv_channel_category[0] in ('FOOD + TRAVEL','HOME + FOOD') then session_duration end) as food_hours, 
#        sum(case when sctv_channel_category[0] in ('FEATURED') then session_duration end) as FEATURED_hours, 
#        sum(case when sctv_channel_category[0] in ('GAME SHOWS','GAME SHOWS + REALITY') then session_duration end) as gameshow_hours, 
#        sum(case when sctv_channel_category[0] in ('GAMING + ANIME') then session_duration end) as gaming_anime_hours, 
#        sum(case when sctv_channel_category[0] in ('HISTORY + DOCUMENTARY') then session_duration end) as HISTORY_DOCUMENTARY_hours, 
#        sum(case when sctv_channel_category[0] in ('HOME','HOME + FOOD') then session_duration end) as home_hours, 
#        sum(case when sctv_channel_category[0] in ('INFOMERCIALS') then session_duration end) as INFOMERCIALS_hours, 
#        sum(case when sctv_channel_category[0] in ('KIDS + FAMILY') then session_duration end) as KIDS_FAMILY_hours, 
#        sum(case when sctv_channel_category[0] in ('LATINO','LATINO [EN ESPANOL]') then session_duration end) as LATINO_hours, 
#        sum(case when sctv_channel_category[0] in ('MOOD + AMBIANCE') then session_duration end) as MOOD_AMBIANCE_hours, 
#        sum(case when sctv_channel_category[0] in ('MOVIES','MOVIES + TV') then session_duration end) as MOVIES_hours, 
#        sum(case when sctv_channel_category[0] in ('MUSIC','MUSIC VIDEOS') then session_duration end) as MUSIC_hours,
#        sum(case when sctv_channel_category[0] in ('NATURE + SCIENCE') then session_duration end) as NATURE_SCIENCE_hours, 
#        sum(case when sctv_channel_category[0] in ('NEWS + OPINION') then session_duration end) as NEWS_OPINION_hours,
#        sum(case when sctv_channel_category[0] in ('REALITY','GAME SHOWS + REALITY') then session_duration end) as REALITY_hours, 
#        sum(case when sctv_channel_category[0] in ('TV','MOVIES + TV') then session_duration end) as TV_hours, 
#        sum(case when sctv_channel_category[0] in ('SPORTS','SPORTS + OUTDOOR') then session_duration end) as SPORTS_hours, 
#        sum(case when sctv_channel_category[0] in ('WESTERNS','WESTERNS + CLASSIC TV') then session_duration end) as WESTERNS_hours,
#        min(datediff(day, session_start::date, {end_date} )) as last_session, 
#        min(case when session_duration >= 180 then datediff(day,  session_start::date, {end_date}) end) as last_3m_session
# from datalake_shares.agg.agg_wfp_live_channel_sessions_v1
# where (date(session_start) between {start_date} and {end_date}) 
# and session_duration is not null
# group by token

# '''

# content_preference = spark.sql(content_prefernece_sql)

# COMMAND ----------

# df = content_preference.join(wfp_usage,"token")

# COMMAND ----------

!pip install yellowbrick

# COMMAND ----------

!pip install --upgrade threadpoolctl

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count, col
from sklearn.cluster import KMeans
from yellowbrick.cluster import KElbowVisualizer,SilhouetteVisualizer
import pandas as pd
import matplotlib.pyplot as plt
import time

import json
import boto3
import seaborn as sns
import numpy as np

from pyspark.sql import functions as F

%matplotlib inline

plt.logging.getLogger('matplotlib.font_manager').disabled = True


# COMMAND ----------

df = spark.sql("select * from dev.mohit_gangwani.wfp_persona_dataset TABLESAMPLE (10 PERCENT) where viewing_hours >= 1")

# COMMAND ----------

# v1_features = [
#     "all_unique_channels",
#     "all_unique_fast_channels",
#     "all_unique_ota_channels",
#     "all_unique_categories",
#     "last_session",
#     "last_3m_session",
#     "num_sessions",
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

# df_subset = df.select([col(c) for c in df.columns if c in v1_features])

# COMMAND ----------

df_subset_pd = df.toPandas().fillna(0)

# COMMAND ----------

df_subset_pd.columns

# COMMAND ----------

v2_features = [
    "all_unique_channels",
    "all_unique_fast_channels",
    "all_unique_ota_channels",
    "all_unique_categories",
    # "CRIME_hours",
    # "FEATURED_hours",
    # "LATINO_hours",
    # "MOVIES_hours",
    # "NEWS_OPINION_hours",
    # "local_content_hours",
    "num_3min_sessions",
    "viewing_hours",
    "fast_hours",
    "ota_hours",
    "avg_distinct_channels_per_session",
    "avg_distinct_fast_channels_per_session",
    "avg_distinct_ota_channels_per_session",
    "avg_distinct_categories_per_session",
    "num_days_active",
]

df_subset_pd = df_subset_pd[v2_features]

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

# for col in df_subset_pd.columns:
#     mn =  round(df_subset_pd[col].mean(), 2)
#     vr =  round(df_subset_pd[col].var(), 2)
#     cnt = np.count_nonzero(df_subset_pd[col])
#     std = round(df_subset_pd[col].std(), 2)
#     rng = round(df_subset_pd[col].max() - df_subset_pd[col].min(), 2)
#     iqr = round(df_subset_pd[col].quantile(0.75) - df_subset_pd[col].quantile(0.25), 2)
#     mdn = round(df_subset_pd[col].median(), 2)
#     skw = round(df_subset_pd[col].skew(), 2)
#     krt = round(df_subset_pd[col].kurt(), 2)
#     print(f'Column: {col}, non_zero_count: {cnt}, Mean: {mn}, Median: {mdn}, Variance: {vr}, Standard_Deviation: {std}, Range: {rng}, IQR: {iqr}, Skewness: {skw}, Kurtosis: {krt}')

# COMMAND ----------

# for col in df_subset_pd.columns:
#     for col2 in df_subset_pd.columns:
#         if col != col2:
#             x = df_subset_pd[col]
#             y = df_subset_pd[col2]
#             plt.figure(figsize = (5,5))
#             plt.scatter(x,y)
#             plt.grid()
#             plt.xlabel(f'{col}',fontsize=10)
#             plt.ylabel(f'{col2}',fontsize=10)
#             plt.show().display()

# COMMAND ----------

# sns.set_palette("Pastel1")

# plt.figure(figsize=(30, 30))

# sns.pairplot(df_subset_pd)

# # plt.suptitle('Pair Plot for DataFrame')
# plt.show()

# COMMAND ----------

plt.figure(figsize=(15, 10))

sns.heatmap(df_subset_pd.corr(), annot=True, cmap="crest", fmt='.2f', linewidths=2)

plt.title('Correlation Heatmap')
plt.show()

# COMMAND ----------

df_subset_pd.info()

# COMMAND ----------

# df_subset_pd.drop(columns=['token', 'gaming_anime_hours'], inplace=True)

# COMMAND ----------

# Instantiate the clustering model and visualizer
km = KMeans(random_state=42)
visualizer = KElbowVisualizer(km, k=(2,15))

visualizer.fit(df_subset_pd)        # Fit the data to the visualizer
visualizer.show()

# COMMAND ----------

# Instantiate the clustering model and visualizer
km = KMeans(random_state=42)
visualizer = SilhouetteVisualizer(km, k=(2,15))

visualizer.fit(df_subset_pd)        # Fit the data to the visualizer
visualizer.show()

# COMMAND ----------

kmeans = KMeans(n_clusters=6, init='k-means++', n_init=25, max_iter=300, random_state=42).fit(df_subset_pd)
df_subset_pd["cluster"] = kmeans.labels_

# COMMAND ----------

kmeans.cluster_centers_

# COMMAND ----------

df_subset_pd.groupby("cluster").mean().reset_index().display()

# COMMAND ----------

df_subset_pd.groupby("cluster").median().reset_index().display()

# COMMAND ----------

pd.plotting.parallel_coordinates(df_subset_pd, 'cluster')

# COMMAND ----------

spark_df = spark.createDataFrame(df_subset_pd)

# COMMAND ----------

spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("dev.mohit_gangwani.wfp_persona_with_labels")

# COMMAND ----------

def gini(x):
    probs = np.bincount(x) / len(x)
    return 1 - np.sum(probs ** 2)

# COMMAND ----------

for i in range(6):
    print(f"Cluster {i}: {len(df_subset_pd[df_subset_pd['cluster'] == i])}")

# COMMAND ----------

gini_df = pd.DataFrame()
for i in range(5, -1, -1):
    for col in df_subset_pd.columns[:-1]:
        gini_df.loc[i, 'cluster'] = f'cluster {i}'
        gini_df.loc[i, col] = gini(df_subset_pd[df_subset_pd['cluster'] == i][col])
        print(f'Cluster {i}, metric: {col} completed')

# COMMAND ----------

def mmr(x):
    return np.mean(x)/np.median(x) if np.median(x) != 0 else 0

# COMMAND ----------

mmr_df = pd.DataFrame()
for i in range(5, -1, -1):
    for col in df_subset_pd.columns[:-1]:
        mmr_df.loc[i, 'cluster'] = f'cluster {i}'
        mmr_df.loc[i, col] = mmr(df_subset_pd[df_subset_pd['cluster'] == i][col])
        print(f'Cluster {i}, metric: {col} completed')

# COMMAND ----------

gini_df.display()

# COMMAND ----------

spark_df = spark.createDataFrame(gini_df)
spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("dev.mohit_gangwani.wfp_persona_gini")
spark_df = spark.createDataFrame(mmr_df)
spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("dev.mohit_gangwani.wfp_persona_mmr")

# COMMAND ----------

mmr_df.display()

# COMMAND ----------

max_retries = 3
retry_delay = 5  # seconds

for attempt in range(max_retries):
    try:
        break
    except ConcurrentAppendException:
        if attempt < max_retries - 1:
            time.sleep(retry_delay)
        else:
            raise

# COMMAND ----------

fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))

centroids = kmeans.cluster_centers_
plotted_combinations = set()  # Use a set for efficient look-up
for i, feature_i in enumerate(v2_features):
    for j, feature_j in enumerate(v2_features[i + 1:], start = i + 1):  # Avoid repeating combinations
        combination_key = f"{feature_i}+{feature_j}"
        if combination_key not in plotted_combinations:
            plotted_combinations.add(combination_key)
            plt.scatter(
                df_subset_pd[feature_i],
                df_subset_pd[feature_j],
                c=df_subset_pd["cluster"],
                cmap="brg",
            )
            for k in range(6):  # Assuming there are 4 centroids
                plt.scatter(
                    centroids[k, i], centroids[k, j], s=200, c="black"
                )  # Adjust indexing for centroids

# Consider saving the figure instead of displaying it directly if running out of memory
# plt.savefig("scatter_plot_matrix.png")
plt.show()  # Uncomment if you want to display the plot inline (may consume a lot of memory)

# COMMAND ----------

fig, ax = plt.subplots(3, 2, figsize=(15, 8))
for i in [2, 3, 4, 5, 6]:
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


