# -*- coding: utf-8 -*-
"""
@author: saifulrijal

Nama: Saiful Rijal
Email: saifulrijal873@gmail.com
"""

import tweepy
import sqlite3
import pandas as pd
import datetime
import re
import numpy as np
import matplotlib.pyplot as plt

def updateData():
    def crawlTweets():
        twitter_keys = {
        'consumer_key': '9VOzyfe8JsLAS2PB5MoYTGciF',
        'consumer_secret': 'NgrJjBhI9p9lpZnoWzG2uGE9DzC9wMjTmB3mP3GbL95Is70CfJ',
        'access_token_key': '797557036099649536-u3suVJFQVSVGgF7s7nrXQfAXoRkrOIT',
        'access_token_secret': 'ymFwuDNObfaLL1o51Y9R7I7d54cQyW4DsqWNax4tTMmOh'}

        auth = tweepy.OAuthHandler(twitter_keys['consumer_key'], twitter_keys['consumer_secret'])
        auth.set_access_token(twitter_keys['access_token_key'], twitter_keys['access_token_secret'])
        api = tweepy.API(auth)
        
        search_words = 'vaksin covid' # fixed keywords
        queries = search_words +'-filter:retweets'
        
        today = datetime.date.today()
        one_Day = datetime.timedelta(days=1)
        date_since = (today - one_Day).strftime('%Y-%m-%d')
        
        tweets = tweepy.Cursor(api.search, q=queries, since=date_since,
                      lang='id', tweet_mode='extended').items()
        items = []
        for tweet in tweets:
            item = []
            item.append(tweet.id_str)
            item.append(tweet.created_at.strftime('%Y-%m-%d'))
            item.append(tweet.user.screen_name)
            item.append(tweet.full_text)
            items.append(item)
            
        return items
    
    def preprocessTweets(items):
        df = pd.DataFrame(items, columns=['tweetID', 'date', 'username', 'tweet'])
        def preprocessText(doc):
            doc = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|('\S*\d\S*')", " ", doc.lower()).split())
            return doc
        df['cleaned_text'] = df['tweet'].apply(preprocessText)
        update_list = df.values.tolist()
        return update_list
    
    def updateDataToDatabase(update_list):
        try:
            conn = sqlite3.connect('twitter_sentiment.db')
            c = conn.cursor()
            print("Connected to SQLite")
            
            create_table = '''CREATE TABLE IF NOT EXISTS twitter_table (
                tweetID TEXT PRIMARY KEY,
                date TEXT,
                username TEXT,
                tweet TEXT,
                cleaned_text TEXT,
                sentiment INTEGER)'''
                
            c.execute(create_table)
            insert_row = '''INSERT OR IGNORE INTO twitter_table (tweetID, date, username, tweet, cleaned_text)
                    VALUES (?,?,?,?,?)'''
            list_row = update_list
            c.executemany(insert_row, list_row)
            
            conn.commit()
            print("Multiple columns updated successfully")
            conn.commit()
            c.close()

        except sqlite3.Error as error:
            print("Failed to update multiple columns of sqlite table", error)
        finally:
            if (conn):
                conn.close()
                print("sqlite connection is closed")
                
    items = crawlTweets()
    print("tweets successfully crawled")
    update_list = preprocessTweets(items=items)
    print("tweets successfully preprocessed")
    updateDataToDatabase(update_list)
    
def updateSentiment():
    def selectSentiNull():
        conn = sqlite3.connect(database='twitter_sentiment.db')
        df = pd.read_sql_query("SELECT * FROM twitter_table WHERE sentiment IS NULL", conn)
        return df

    def sentimentScoring(data):
        with open('./kata_positif.txt', 'r') as f:
            pos_list = [line.strip() for line in f]
        with open('./kata_negatif.txt', 'r') as f:
            neg_list = [line.strip() for line in f]
    
        def sentiment(doc):
            senti=0
            words = [word for word in doc.split()]
            for word in words:
                if word in pos_list:
                    senti += 1
                elif word in neg_list:
                    senti -= 1
            return senti
        
        df['sentiment'] = df['cleaned_text'].apply(sentiment)
        update_list = df[['sentiment', 'tweetID']].values.tolist()
        return update_list
    
    def updateSentiToDatabase(update_list):
        try:
            conn = sqlite3.connect('twitter_sentiment.db')
            c = conn.cursor()
            print("Connected to SQLite")

            sqlite_update_query = """UPDATE twitter_table SET sentiment = ? where tweetID = ?"""
            list_row = update_list
            c.executemany(sqlite_update_query, list_row)
            conn.commit()
            print("Multiple columns updated successfully")
            conn.commit()
            c.close()

        except sqlite3.Error as error:
            print("Failed to update multiple columns of sqlite table", error)
        finally:
            if (conn):
                conn.close()
                print("sqlite connection is closed")
        
    df = selectSentiNull()
    update_list = sentimentScoring(data=df)
    print("Sentiment score successfully added")
    updateSentiToDatabase(update_list=update_list)
    
def displayData(since_date, until_date):
    conn = sqlite3.connect('twitter_sentiment.db')
    query = """SELECT username, date, tweet FROM twitter_table WHERE date BETWEEN '{}' AND '{}'""".format(since_date, until_date)
    df = pd.read_sql_query(query, conn)
    return print(df)

def visualizeData(since_date, until_date):

    def toDataFrame(since_date, until_date):
        conn = sqlite3.connect('twitter_sentiment.db')
        query = """SELECT username, date, tweet, sentiment FROM twitter_table WHERE date BETWEEN '{}' AND '{}'""".format(since_date, until_date)
        df = pd.read_sql_query(query, conn)
        return df
    
    df = toDataFrame(since_date=since_date, until_date=until_date)
    mean = np.mean(df["sentiment"])
    median = np.median(df["sentiment"])
    std_dev = np.std(df["sentiment"])
    
    print("mean: "+str(mean))
    print("median: "+str(median))
    print("standard deviation: "+str(std_dev))
    
    labels, counts = np.unique(df["sentiment"], return_counts=True)
    plt.bar(labels, counts, align='center')
    plt.gca().set_xticks(labels)
    plt.show()

number = input("""This is a simple Twitter sentiment application with the keyword 'vaksin covid'.
What do you want to do?
1. Update Data
2. Update Sentiment Score
3. Display Data
4. Visualize Sentiment
5. Exit
select the number: """)
               
if number == '1':
    updateData()
elif number == '2':
    updateSentiment()
elif number == '3':
    since_date = input("since date format(yyyy-mm-dd): ")
    until_date = input("until date format(yyyy-mm-dd): ")
    displayData(since_date=since_date, until_date=until_date)
elif number == '4':
    since_date = input("since date format(yyyy-mm-dd): ")
    until_date = input("until date format(yyyy-mm-dd): ")
    visualizeData(since_date=since_date, until_date=until_date)
elif number == '5':
    print("Thanks for trying")
    exit()
else:
    print("please enter the appropriate number")
    