import pymongo
import pandas as pd
import numpy as np
from pymongo import MongoClient
from textblob import TextBlob
import time

# Provide MongoDB Atlas credentials and details
dbname = 'dbname'
collectionname = 'collectionname'
sentiment_collectionname = 'sentiment_collectionname'
uri = 'uri'

# Connect to the MongoDB Atlas cluster
client = MongoClient(uri)

# Create a collection object for your Reddit posts time-series data
collection = client[dbname][collectionname]

# Create a collection object for the result
sentiment_collection = client[dbname][sentiment_collectionname]

# Define a function to perform sentiment analysis on a single Reddit post using TextBlob
def get_sentiment(text):
    if isinstance(text, str):
        blob = TextBlob(text)
        return blob.sentiment.polarity
    else:
        return 0

# Get latest document _id to track new posts
latest_doc_id = collection.find_one(sort=[('_id', pymongo.DESCENDING)])['_id']

while True:
    new_posts = list(collection.find({'_id': {'$gt': latest_doc_id}}))
    
    if new_posts:
        latest_doc_id = new_posts[-1]['_id']

        # Convert new_posts into a DataFrame
        df = pd.DataFrame(new_posts)

        # Apply the get_sentiment function to the title and description columns of the DataFrame to calculate the sentiment score for each Reddit post
        df['title_sentiment'] = df['title'].apply(get_sentiment)
        df['description_sentiment'] = df['description'].apply(get_sentiment)

        # Check the type of the comments field
        if df['comments'].apply(lambda x: isinstance(x, list)).all():
            # If the comments field contains a list of strings
            df['comment_sentiment'] = df['comments'].apply(lambda comments: sum([get_sentiment(comment) for comment in comments])/len(comments) if comments else np.nan)
        else:
            # If the comments field contains something other than a list of strings
            df['comment_sentiment'] = None

        # Merge the sentiment scores from the three columns and take the average
        df['sentiment_score'] = df[['title_sentiment', 'description_sentiment', 'comment_sentiment']].mean(axis=1)

        # Add the authors to the DataFrame
        df['authors'] = df['author'] + ', ' + df['comments'].apply(lambda comments: ', '.join(set([comment['author'] for comment in comments])) if comments else '')

        # Convert DataFrame to dictionary and write to the sentiment_collection
        sentiment_collection.insert_many(df.to_dict('records'))

        # Wait for a while before checking for new posts again
        time.sleep(5)

client.close()
