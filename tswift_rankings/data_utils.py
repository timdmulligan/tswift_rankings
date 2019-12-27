import requests
from bs4 import BeautifulSoup
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials


def web_to_df(base_url: str) -> pd.DataFrame:
    df = pd.DataFrame([])
    for page in range(1, 5):
        page_request = requests.get(f"{base_url}/?list_page={page}")
        soup = BeautifulSoup(page_request.content, "html.parser")
        df_dict = {
            "rank": [
                x.get_text().replace("\n", "").replace("\t", "")
                for x in soup.find_all("span", "c-list__number t-bold")
            ],
            "song": [
                x.get_text().replace("\n", "").replace("\t", "")
                for x in soup.find_all("h3", "c-list__title t-bold")
            ],
            "desc": [
                x.get_text().replace("\n", "").replace("\t", "")
                for x in soup.find_all("div", "c-list__lead c-content")[1:]
            ],
        }
        df = df.append(pd.DataFrame(df_dict))
    df["year"] = list(
        map(lambda x: x.split(" ")[-1].replace("(", "").replace(")", ""), df["song"])
    )
    df["title"] = list(map(lambda x: " ".join(x.split(" ")[:-1]), df["song"]))
    df["title"] = (
        df["title"]
        .str.split("” ")
        .apply(lambda x: x[0])
        .str.split(" \(")
        .apply(lambda x: x[0])
        .str.lower()
    )
    df["title"] = (
        df["title"]
        .str.replace("&", "and")
        .str.replace("’", "")
        .str.replace("?", "")
        .str.replace("…", "")
        .str.replace("“", "")
        .str.replace("”", "")
    )
    return df


def append_spotify_to_df(dataframe: pd.DataFrame, credentials: dict) -> pd.DataFrame:
    client_credentials_manager = SpotifyClientCredentials(
        client_id=credentials["client_id"], client_secret=credentials["client_secret"]
    )
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    album_name = []
    release_date = []
    track_number = []
    popularity = []
    for song in dataframe["title"]:
        results = sp.search(q=f"{song} Taylor Swift", limit=1)
        if len(results["tracks"]["items"]) == 1:
            album_name += [results["tracks"]["items"][0]["album"]["name"]]
            release_date += [results["tracks"]["items"][0]["album"]["release_date"]]
            track_number += [results["tracks"]["items"][0]["track_number"]]
            popularity += [results["tracks"]["items"][0]["popularity"]]
        else:
            album_name += [None]
            release_date += [None]
            popularity += [None]
            track_number += [None]

    dataframe["album_name"] = album_name
    dataframe["release_date"] = release_date
    dataframe["track_number"] = track_number
    dataframe["popularity"] = popularity
    return dataframe


def append_wikipedia_to_df(dataframe: pd.DataFrame, path: "str") -> pd.DataFrame:
    writers = pd.read_csv(path)
    writers["title"] = (
        writers["Song"]
        .str.replace('"', "")
        .str.split(" \(")
        .apply(lambda x: x[0])
        .str.lower()
    )
    writers.drop(["Year"], axis=1, inplace=True)
    writers["title"] = (
        writers["title"]
        .str.replace("&", "and")
        .str.replace("'", "")
        .str.replace("?", "")
        .str.replace("…", "")
        .str.replace(".", "")
    )
    return dataframe.merge(writers, on="title", how="inner")

