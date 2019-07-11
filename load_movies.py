import pandas as pd

from es_api import EsApi

es = EsApi()

movie_df = pd.read_csv("./movies.csv")
# data.columns


def get_year(arrlick, column):
    year = arrlick[column].split("(")[-1].split(")")[0]
    return year


def clear_gener(arrlick, column):
    gener = arrlick[column].split("|")
    return gener


print(movie_df.columns)


movie_df["year"] = movie_df.apply(get_year, axis=1, args=("title", ))
movie_df["genres"] = movie_df["genres"].apply(lambda x: x.split("|"))
movie_df["title"] = movie_df["title"].apply(lambda x: x.split("(")[0])


# print(movie_df[["title"]])
# print(movie_df.columns)


for row in movie_df.itertuples():
    # print(row[1], row[2], row[3], row[4])
    data = {"id": row[1], "title": row[2], "genre": row[3], "year": row[4]}
    es.insert(data)
