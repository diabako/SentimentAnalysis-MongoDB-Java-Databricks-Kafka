# Leveraging MongoDB Atlas, Kafka Confluent Cloud, and Databricks To Perform Reddit and StackOverflow Posts Sentiment Analysis - Part 2

## Overview

This is the second part of my real-time sentiment analysis project. In this part, I have added the following enhancements:

- Integrated [Kafka Confluent Cloud](https://confluent.cloud/signup) to enable real-time data streaming from [Reddit](https://www.reddit.com/) and [StackOverflow](https://stackoverflow.com/)
- Added support for analyzing StackOverflow posts related to MongoDB using [StackOverflow API](https://api.stackexchange.com/)
- Improved the sentiment analysis code in [Databricks](https://www.databricks.com/) for near real-time processing
- Created more detailed visualizations using [MongoDB Charts](https://www.mongodb.com/docs/charts/)

## Architecture

The high-level architecture now looks like this:

<img width="708" alt="Screenshot 2023-09-10 at 21 02 06" src="https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks-Kafka/assets/84781155/1f0ccc15-f17b-4c34-8081-aab2a134cc97">

## Getting Started

### Prerequisites

- [MongoDB Atlas M0 cluster](https://www.mongodb.com/developer/products/atlas/free-atlas-cluster/)
- [Confluent Cloud Kafka cluster](https://docs.confluent.io/cloud/current/get-started/free-trial.html#free-trial)
- [Reddit](https://www.reddit.com/dev/api/) and [StackOverflow](https://api.stackexchange.com/) developer APIs access 
- [Databricks workspace](https://www.databricks.com/resources/webinar/databricks-on-aws-free-training-series?scid=7018Y000001Fi0oQAC&utm_medium=paid+search&utm_source=google&utm_campaign=17882588532&utm_adgroup=148019617148&utm_content=od+webinar&utm_offer=databricks-on-aws-free-training-series&utm_ad=665998293355&utm_term=databricks%20aws&gclid=Cj0KCQjw9fqnBhDSARIsAHlcQYTweaot4TwhBit65UcHOCdLH346lNUpp9HX9ZuYs3trciuIfWKUhjgaAmQqEALw_wcB)
- [A Java Maven setup](https://www.mongodb.com/developer/languages/java/java-setup-crud-operations/?utm_campaign=javainsertingdocuments&utm_source=facebook&utm_medium=organic_social)

### Setup Steps

1. **Set up Kafka and producers**

   - Create Confluent Cloud cluster and note broker IPs
   - Create a Kafka topic
   - Add bootstrap servers to producers
   - Run producers to start streaming data
  
The code in PostToKafka.java shared in this git repository provides an example of code to push the Reddit and StackOverflow to Kafka Confluent Cloud cluster. Make sure to update it with your own confluent cloud cluster details and topic.

2. **Create MongoDB Atlas cluster**

   - Create M0 cluster, you can find the steps on how to create the cluster [here](https://www.mongodb.com/developer/products/atlas/free-atlas-cluster/)
   - Create two time series collections - one for raw posts, one for the curated data. More details on how to create time series collection can be found in this [documentation](https://www.mongodb.com/docs/manual/core/timeseries/timeseries-procedures/#std-label-timeseries-create-query-procedures)

3. **Configure Kafka MongoDB connector**

   - In Confluent Cloud, add MongoDB Atlas sink connector
     
  <img width="1127" alt="Screenshot 2023-09-10 at 16 00 48" src="https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks-Kafka/assets/84781155/22ed8659-c8a6-4e7a-a0dc-a764ccfb7798">
  
   - Make sure to select the topic and provide the details of the Confluent Cloud cluster API configured in step 1
   - Configure connection string and database name

 <img width="847" alt="Screenshot 2023-09-12 at 07 47 12" src="https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks-Kafka/assets/84781155/10e9d590-825a-4950-b257-81d0b1fbc3a3">
 
   - Make sure to update the configuration specific for time series collection adding the timefield details and the write strategy (I chose the insert one strategy to avoid some time series limitations I encountered before).

<img width="919" alt="Screenshot 2023-09-10 at 16 19 30" src="https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks-Kafka/assets/84781155/7356dee9-9b39-4158-88bb-602eb188c0ec">

4. **Update Databricks notebook**

   - Create a new Databricks workspace or use an existing one
   - In the Databricks workspace, create a new cluster and configure it as per your requirements
   - Create a new notebook in Databricks and configure it to use the Python programming language.
   - Install the PyMongo package in your Databricks notebook using the following command:

```python
!pip install pymongo
```
   - Install the TextBlob package in your Databricks notebook using the following command:

```python
!pip install textblob
```

   - Make sure to retrieve your Databricks cluster IP and add it to MongoDB Atlas Allowlist.
   - Define a function to perform sentiment analysis on a single post using TextBlob

```python
def get_sentiment(text):
    if isinstance(text, str):
        blob = TextBlob(text)
        return blob.sentiment.polarity
    else:
        return None
```
- I will be using the above function to perform sentiment analysis on new posts that land on MongoDB raw posts collection before pushing the results into the curated collection.

5. **Create visualizations on MongoDB Atlas Charts**

Use [MongoDB Atlas Charts](https://www.mongodb.com/products/charts) to visualize the sentiment analysis results. You can create various types of charts to understand the sentiment of the Reddit data over time. For example, in this project, we created a chart to visualize the average post-sentiment score over time. Here are the steps to build that chart:

- First, log in to MongoDB Atlas and navigate to your cluster.
- Click on the "Charts" button.
- If you haven't used Atlas Charts before, you might need to set it up first. Just follow the on-screen instructions to get started.
- To get you started faster, you will find a simplistic pre-defined dashboard with a few charts already built. You can follow these [steps](https://www.mongodb.com/blog/post/import-export-your-charts-dashboards#:~:text=To%20export%20a%20dashboard%2C%20simply,menu%20next%20to%20Add%20Dashboard.) to import that dashboard into your MongoDB Atlas project.

### Code Samples

Key code samples are provided in the following files:

- `PostsToKafka.java`: Fetches Reddit posts and StackOverflow posts and streams to Kafka 
- `sentimentAnalysis.py`: Performs real-time sentiment analysis

### Visualizations 

The `dashboard.json` file contains a pre-configured dashboard with:

- Sentiment score over time
- Sentiment by source
- Sentiment comparison between Reddit and analysis

## Resources

- [Part 1 of this project](https://github.com/diabako/SentimentAnalysis-MongoDB-Java-Databricks)
- [Blog post for this project](https://www.example.com/blog)
