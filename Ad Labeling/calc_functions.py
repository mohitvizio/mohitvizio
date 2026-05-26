import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def gini(ad):
    x = np.sort(np.array(ad))
    n = x.size
    if n <= 1 or np.sum(x) == 0:
        return 1.0

    index = np.arange(1, n + 1)
    a = (2.0 * np.sum(index * x))/(n * np.sum(x))
    c = (n + 1) / n
    g = a - c
    norm = n / (n - 1)
    return norm * g


def locality_index(values):
    if len(values) <= 1 or np.sum(values) == 0:
        return 1.0
    p = np.array(values) / np.sum(values)
    p = p[p > 0]
    H = -np.sum(p * np.log(p))
    return 1 - (H / np.log(len(p)))


def normalized_gini(p):
    p = np.sort(np.array(p))
    if len(p) <= 1:
        return 1.0
    p = p[p > 0]
    H = -np.sum(p * np.log(p))
    H_norm = H / np.log(len(p))
    return H_norm


def normalized_entropy(p):
    p = np.sort(np.array(p))
    n = p.size
    if n <= 1 or np.sum(p) <= 0.0:
        return 0.0

    a = (2 * np.sum(np.arange(1, n + 1) * p)) / (n*np.sum(p))
    b = (n+1)/n
    g = a - b
    norm = n / (n - 1)
    return norm * g


def scatterplot(df, x, y, title, xlabel, ylabel):
    sns.scatterplot(
        data=df, 
        x=x,
        y=y,
        alpha=0.35, 
        s=5
    )
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()


def kde_plot(df, x, y, title, xlabel, ylabel):
    sns.kdeplot(
        data=df,
        x=x,
        y=y,
        fill=True,
        alpha=0.4
    )
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.show()
