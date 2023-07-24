# [DRAFT] Leveraging MongoDB Atlas, Kafka Confluent Cloud, and Databricks To Perform Reddit and StackOverflow Posts Sentiment Analysis - Part 2


In the [previous part]([URL_to_part_1](https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks)) of this tutorial, we explored how to extract posts from Reddit, store them into a MongoDB Atlas Time Series collection, and perform sentiment analysis using a Databricks notebook. In this part, we're going to enrich our project by adding real-time capabilities using Kafka Confluent Cloud. Additionally, we'll include another data source - StackOverflow, to broaden the scope of our analysis.

This guide will take you through the process of:

1. Sending posts from Reddit and StackOverflow to Kafka Confluent Cloud
2. Using the Kafka Connector to send all data to MongoDB Atlas Time Series collection
3. Connecting a Databricks notebook to run sentiment analysis on those posts in near real-time
4. Leveraging MongoDB Charts in order to visualize the results of the sentiment analysis

The objective of these enhancements is to perform a more comprehensive analysis and tackle some of the limitations we previously encountered, such as the Reddit API rate limit and the inability of MongoDB Time Series collection to handle change streams (as of MongoDB V6).

## Prerequisites

- A Reddit account
- A MongoDB Atlas account
- A Kafka Confluent Cloud account
- A Databricks account

## High-Level Steps

1. Setup Kafka Confluent Cloud
2. Create producers for Reddit and StackOverflow posts
3. Create a Kafka Connector to MongoDB Atlas
4. Configure the Kafka Connector
5. Update the Databricks script to run in near real-time
6. Visualize the results using MongoDB Atlas Chart

## Detailed Steps

### 1. Setup Kafka Confluent Cloud

Sign up for a Kafka Confluent Cloud account if you haven’t done so already. After signing up, follow the onboarding wizard to create a new cluster.

### 2. Create Producers for Reddit and StackOverflow Posts

I used the Java programming language to interact with the Reddit and StackOverflow APIs and send the posts to Kafka Confluent Cloud.

### 3. Create a Kafka Connector to MongoDB Atlas

Go to your Kafka Confluent Cloud dashboard and navigate to the Connectors section. Click on "Add Connector" and choose MongoDB Atlas Sink from the available options.

### 4. Configure the Kafka Connector
Configure your Kafka Connector to consume from the 'reddit_posts' and 'stackoverflow_posts' topics and sink them into your MongoDB Atlas collection.

Key configurations:

Connection.uri: Your MongoDB Atlas connection string
Topics: 'reddit_posts,stackoverflow_posts'
Mongo.writemodel.strategy: Replace if a document with the same _id exists or Insert otherwise.

### 5. Update the Databricks Script to Run in Near Real-Time
Finally, modify your Databricks notebook to fetch and analyze the posts from MongoDB Atlas in near real-time.

### 6. Configure MongoDB Atlas Chart in order to visualize the sentiment analysis

With these enhancements, your sentiment analysis tool now operates in near real-time, giving you an even more accurate understanding of developers' sentiment towards MongoDB, as expressed on Reddit and StackOverflow.

