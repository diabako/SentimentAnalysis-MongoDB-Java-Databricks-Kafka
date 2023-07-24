import pymongo
import pandas as pd
from pymongo import MongoClient
from textblob import TextBlob
import time

# Provide MongoDB Atlas credentials and details
dbname = 'dbname'
collectionname = 'collectionname'
sentiment_collectionname = 'sentiment_collectionname'
uri = 'mongodb+srv://USERNAME:PASSWORD@CLUSTER-URI/?retryWrites=true&w=majority'

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

def process_posts(posts):
    # Convert posts into a DataFrame
    df = pd.DataFrame(posts)

    # Apply the get_sentiment function to the title, description, and comments columns of the DataFrame to calculate the sentiment score for each post
    if 'title' in df.columns:
        df['title_sentiment'] = df['title'].apply(get_sentiment)
    if 'description' in df.columns:
        df['description_sentiment'] = df['description'].apply(get_sentiment)
    if 'comments' in df.columns:
        if df['comments'].apply(lambda x: isinstance(x, list)).all():
            # If the comments field contains a list of strings
            df['comment_sentiment'] = df['comments'].apply(lambda comments: sum([get_sentiment(comment) for comment in comments])/len(comments) if comments else None)
        else:
            # If the comments field contains something other than a list of strings
            df['comment_sentiment'] = None

    # Create a list of sentiment fields that actually exist
    sentiment_fields = [field for field in ['title_sentiment', 'description_sentiment', 'comment_sentiment'] if field in df.columns]

    # Merge the sentiment scores from the three columns and take the average
    df['sentiment_score'] = df[sentiment_fields].mean(axis=1)

    if 'author' in df.columns:
        def process_comments(comments):
            if isinstance(comments, list) and all(isinstance(comment, dict) and 'author' in comment for comment in comments):
                return ', '.join(set([comment['author'] for comment in comments]))
            else:
                return ''

        df['authors'] = df['author'].apply(lambda x: str(x) if pd.notnull(x) else '') + ', ' + df['comments'].apply(process_comments)

    # Convert DataFrame to dictionary and write to the sentiment_collection
    sentiment_collection.insert_many(df.to_dict('records'))

# Process all posts the first time
all_posts = list(collection.find({}))
process_posts(all_posts)

# Get the latest document _id to track new posts
latest_doc_id = all_posts[-1]['_id']

while True:
    # Get new posts
    new_posts = list(collection.find({'_id': {'$gt': latest_doc_id}}))
    
    if new_posts:
        # Update latest_doc_id with the _id of the latest document fetched
        latest_doc_id = new_posts[-1]['_id']

        # Process new posts
        process_posts(new_posts)

    # Wait for a while before checking for new posts again
    time.sleep(5)

client.close()
