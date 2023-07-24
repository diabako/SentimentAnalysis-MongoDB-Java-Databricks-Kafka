import pymongo
from pymongo import MongoClient
from textblob import TextBlob
import time

# Provide MongoDB Atlas credentials and details
dbname = 'dbname'
collectionname = 'collectionname'
sentiment_collectionname = 'sentiment_collectionname'
uri = 'mongodb+srv://USERNAME:PASSWORD@ATLAS-CLUSTER-URI/?retryWrites=true&w=majority'

# Connect to the MongoDB Atlas cluster
client = MongoClient(uri)

# Create a collection object for your posts time-series data
collection = client[dbname][collectionname]

# Create a collection object for the result
sentiment_collection = client[dbname][sentiment_collectionname]

# Define a function to perform sentiment analysis on a single post using TextBlob
def get_sentiment(text):
    if isinstance(text, str):
        blob = TextBlob(text)
        return blob.sentiment.polarity
    else:
        return None

def process_post(post):
    sentiments = []
    for field in ['title', 'description', 'comments']:
        if field in post and post[field]:
            if field == 'comments' and isinstance(post[field], list):
                sentiments.extend([get_sentiment(comment) for comment in post[field] if get_sentiment(comment) is not None])
            else:
                sentiment = get_sentiment(post[field])
                if sentiment is not None:
                    sentiments.append(sentiment)
    post['sentiment_score'] = sum(sentiments)/len(sentiments) if sentiments else None
    return post


# Process all posts the first time
all_posts = list(collection.find({}))

sentiment_collection.insert_many([process_post(post) for post in all_posts])

# Get the latest document _id to track new posts
latest_doc_id = all_posts[-1]['_id']

while True:
    # Get new posts
    new_posts = list(collection.find({'_id': {'$gt': latest_doc_id}}))
    
    if new_posts:
        # Update latest_doc_id with the _id of the latest document fetched
        latest_doc_id = new_posts[-1]['_id']

        # Process new posts
        sentiment_collection.insert_many([process_post(post) for post in new_posts])

    # Wait for a while before checking for new posts again
    time.sleep(5)

client.close()
