import json
import socket
from time import gmtime, strftime
import tweepy
import csv
from datetime import datetime


# Set up your credentials

MY_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAADy4eAEAAAAA8oN2oHIfqe7smVNzVygz0hPsbEE" \
                  "%3DncksucgIdic3iYgFaviEOLtzsCn32qnVX2m7oAiPADt85HGFzF "


def get_tweets(hastag):
    client = tweepy.Client(bearer_token=MY_BEARER_TOKEN)
    # query to search for tweets
    query = f"{hastag} lang:fr -is:retweet"
    # get tweets from the API
    tweets = client.search_recent_tweets(query=query,
                                         tweet_fields=["created_at", "text", "source", "lang", "public_metrics",
                                                       "entities"],
                                         user_fields=["name", "username", "location", "verified", "description"],
                                         max_results=100,
                                         expansions='author_id')

    header = ['id', 'created_at', 'lang', 'source', "like", "retweet", 'hashtags0', 'hashtags1', 'hashtags2']

    full = []
    for i in range(100):
        # 'created_at', 'id', 'text', 'source', 'author_id'
        id = tweets.data[i].data['id']
        created_at = datetime.strptime(tweets.data[i].data['created_at'], "%Y-%m-%dT%H:%M:%S.%f%z").strftime("%Y-%m"
                                                                                                             "-%d "
                                                                                                             "%H:%M")
        lang = tweets.data[i].data['lang']
        #text = tweets.data[i].data['text']
        source = tweets.data[i].data['source']
        like = tweets.data[i].data['public_metrics']['like_count']
        retweet = tweets.data[i].data['public_metrics']['retweet_count']
        hashtags0 = tweets.data[i].data['entities']['hashtags'][0]['tag']
        hashtags1 = ""
        hashtags2 = ""
        if len(tweets.data[i].data['entities']['hashtags']) == 2:
            hashtags1 = tweets.data[i].data['entities']['hashtags'][1]['tag']

        if len(tweets.data[i].data['entities']['hashtags']) == 3:
            hashtags2 = tweets.data[i].data['entities']['hashtags'][2]['tag']

        liste = [id, created_at, lang, source, like, retweet, hashtags0, hashtags1, hashtags2]

        full.append(liste)

    with open(f'/Users/fabienbarrios/Desktop/Cours/Spark/ProjetSpark/sparkProjet/Dataset/{strftime("%Y-%m-%d_%H_%M", gmtime())}.csv', 'w', encoding='UTF8',
              newline='') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)

        # write multiple rows
        writer.writerows(full)


if __name__ == "__main__":
    get_tweets("#covid19")
