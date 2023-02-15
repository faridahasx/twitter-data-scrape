import tweepy as tp
import pandas as pd
import numpy as np
from binance.client import Client
from datetime import datetime as dt
import finplot as fplt
import time 
from constants import *


def connect_twitter(api_key, secret_key, access_token, secret_access_token):
    auth = tp.OAuthHandler(
        consumer_key=api_key,
        consumer_secret=secret_key
    )
    auth.set_access_token(
        key=access_token,
        secret=secret_access_token
    )
    api = tp.API(auth)
    try:
        api.verify_credentials()
        print("Connection to Twitter established.")
    except Exception as e:
        print("Failed to connect to Twitter.")
        print(e)
        quit()

    return api


def get_all_tweets(screen_name, api):
    all_tweets = []

    new_tweets = api.user_timeline(screen_name=screen_name, count=200)
    all_tweets.extend(new_tweets)
    oldest = all_tweets[-1].id - 1

    while len(new_tweets) > 0:
        new_tweets = api.user_timeline(screen_name=screen_name, count=200, max_id=oldest)
        all_tweets.extend(new_tweets)
        oldest = all_tweets[-1].id - 1

    # to a 2D array
    outtweets = [
        [
            t.author.name, t.id_str, t.created_at, t.text, t.entities.get('hashtags'), t.author.location,
            t.author.created_at, t.author.url, t.author.screen_name, t.favorite_count, t.favorited,
            t.retweet_count, t.retweeted, t.author.followers_count, t.author.friends_count
        ] for t in all_tweets
    ]

    # to csv
    cols = [
        'author_name', 'tweet_id', 'tweet_created_at', 'content', 'hashtags', 'location',
        'author_created_at', 'author_url', 'author_screen_name', 'tweet_favourite_count', 'tweet_favourited',
        'retweet_count', 'retweeted', 'author_followers_count', 'author_friends_count'
    ]
    df = pd.DataFrame(outtweets, columns=cols)
    df.to_csv(f'./data/{screen_name}.csv', index=False)
    time.sleep(10)
    return df


def analyse_tweets(df):
    dates = []

    for x in df.itertuples(index=False):
        content = str(x.content).lower()
        exclude_tweet = False
        for e in exclude:
            e = e.lower()
            if e in content:
                exclude_tweet = True
                break

        if not exclude_tweet:
            for i in include:
                i = i.lower()
                if i in content:
                    date = dt.strptime(x.tweet_created_at[0:-6], '%Y-%m-%d %H:%M:%S')
                    dates.append(date)
                    print(f'{date}: {x.content}')
                    break
    return dates


def get_price_data(pair):
    start_date = '1 Dec, 2015'
    end_date = '1 Jan, 2090'

    # To change timeframe, change interval ---
    # e.g., for 1 Minute: interval = Client.KLINE_INTERVAL_1MINUTE
    interval = Client.KLINE_INTERVAL_4HOUR

    client = Client(binance_api_key, binance_secret_key)
    candlesticks = client.get_historical_klines(pair, interval, start_date, end_date)
    # To Dataframe
    df = pd.DataFrame(candlesticks)
    df['date'] = pd.to_datetime(df[0] / 1000, unit='s')
    df[['open', 'high', 'low', 'close', 'volume']] = df[[1, 2, 3, 4, 5]].apply(pd.to_numeric)
    df = df[['date','open', 'high', 'low', 'close', 'volume']]
    df.to_csv(f'price_data/{pair}.csv', index=False)


# Connect to twitter
api = connect_twitter(
    api_key=twitter_api_key,
    secret_key=twitter_secret,
    access_token=twitter_access_token,
    secret_access_token=twitter_access_token_secret)

# Get Tweets
print('Scraping tweets. . .')
get_all_tweets(username, api)

# Get Price Data
print('Getting Price Data. . .')
get_price_data(binance_pair)

# Analyse Tweets
print('Analysing tweets with the keywords. . .\n')
df_tweets = pd.read_csv(f'data/{username}.csv')
dates = analyse_tweets(df_tweets)

# Analyse price data
df_price = pd.read_csv(f'price_data/{binance_pair}.csv')
df_price["signal"] = np.nan
df_price['date'] = pd.to_datetime(df_price['date'])
for date in dates:
    df_price.loc[((df_price["date"] < date) & (date < df_price["date"].shift(-1))) | (df_price["date"] == date), "signal"] = 1
print('\nDone!')

## PLOT
ax = fplt.create_plot(binance_pair)
candles = df_price[['date', 'open', 'close', 'high', 'low']]
fplt.candlestick_ochl(candles, ax=ax)
volumes = df_price[['date', 'open', 'close', 'volume']]
fplt.volume_ocv(volumes, ax=ax.overlay())

df_price.loc[df_price["signal"] == 1, "marker"] = df_price['low']
fplt.plot(df_price['date'], df_price['marker'], ax=ax, color='#4a1', style='^', legend='tweets')
fplt.show()
