package com.mongodb.sentiment;
import com.google.gson.JsonObject;
import net.dean.jraw.RedditClient;
import net.dean.jraw.http.UserAgent;
import net.dean.jraw.models.Comment;
import net.dean.jraw.models.Submission;
import net.dean.jraw.models.SubredditSort;
import net.dean.jraw.oauth.Credentials;
import net.dean.jraw.oauth.OAuthHelper;
import net.dean.jraw.pagination.DefaultPaginator;
import net.dean.jraw.references.SubmissionReference;
import net.dean.jraw.tree.CommentNode;
import net.dean.jraw.tree.RootCommentNode;
import net.dean.jraw.models.PublicContribution;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.*;
import com.google.gson.JsonArray;
import org.bson.Document;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RedditPostsToKafka {
    public static void main(String[] args) {
        // Reddit API credentials
        String clientId = "nEuQlaEBe5Ofrq8uuI1Hww";
        String clientSecret = "2dR1_Ovkh3oGZDqOLrlcJf_fP3eKrA";
        String username = "WorkingBot2501";
        String password = "semanticerror2501";

        // Set up JRAW with your Reddit API credentials
        UserAgent userAgent = new UserAgent("bot", "com.example.reddit", "v0.1", "your_reddit_username");
        Credentials credentials = Credentials.script(username, password, clientId, clientSecret);
        RedditClient redditClient = OAuthHelper.automatic(new net.dean.jraw.http.OkHttpNetworkAdapter(userAgent), credentials);

        // Kafka configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092"); // replace <cluster-id> with your Confluent Cloud cluster ID
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='QOSOV73O2FDYWCGF' password='0ikMRHijAAdALwPnMsnzLMZl4n4REp4RicqatG8rBSQyd8TaGXJ0L0LwAJFVTxqn';"); // replace <api-key> and <api-secret> with your Confluent Cloud API key and secret
        props.put("sasl.mechanism", "PLAIN");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
        // Define the subreddit you want to fetch posts from
        String subredditName = "mongodb";

        // Schedule a task to fetch new posts and comments every 60 seconds
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                fetchNewPostsAndComments(redditClient, subredditName, kafkaProducer);
            }
        }, 0, 60 * 1000);
    }

    public static void fetchNewPostsAndComments(RedditClient redditClient, String subredditName, Producer<String, String> kafkaProducer) {
        // Fetch new posts from the specified subreddit
        DefaultPaginator<Submission> paginator = redditClient.subreddit(subredditName).posts().sorting(SubredditSort.NEW).limit(50).build();

        // Iterate through the fetched posts and store them in the Kafka topic along with their comments
        for (Submission post : paginator.next()) {
            // Create a unique identifier for the post
            String postHash = Hashing.sha256().hashString(post.getTitle() + post.getAuthor(), StandardCharsets.UTF_8).toString();

            // Create a new JSON document for the post and its comments
            JsonObject postJson = new JsonObject();
            postJson.addProperty("id", post.getId());
            postJson.addProperty("title", post.getTitle());
            postJson.addProperty("author", post.getAuthor());
            postJson.addProperty("score", post.getScore());
            postJson.addProperty("permalink", post.getPermalink());
            postJson.addProperty("url", post.getUrl());
            postJson.addProperty("description", post.getSelfText());
            postJson.addProperty("created", post.getCreated().getTime());

            JsonArray commentsJson = new JsonArray();
            for (Document comment : getCommentsAsDocuments(redditClient, post.getId())) {
                JsonObject commentJson = new JsonObject();
                commentJson.addProperty("id", comment.getString("id"));
                commentJson.addProperty("author", comment.getString("author"));
                commentJson.addProperty("body", comment.getString("body"));
                commentJson.addProperty("score", comment.getInteger("score"));
                commentJson.addProperty("created", comment.getLong("created"));
                commentsJson.add(commentJson);
            }
            postJson.add("comments", commentsJson);

            // Convert the JSON document to a string and send it to the Kafka topic
            String postJsonString = postJson.toString();
            kafkaProducer.send(new ProducerRecord<String, String>("reddit_posts_topic", postHash, postJsonString));

            System.out.println("Stored post and comments: " + post.getTitle());
        }
    }

    public static List<Document> getCommentsAsDocuments(RedditClient redditClient, String postId) {
        List<Document> comments = new ArrayList<>();

        // Fetch comments for the post
        SubmissionReference submissionRef = redditClient.submission(postId);
        RootCommentNode root = submissionRef.comments();

        // Iterate through the comments and add them to the list
        Iterator<? extends CommentNode<? extends PublicContribution<?>>> iterator = root.walkTree().iterator();
        while (iterator.hasNext()) {
            CommentNode<? extends PublicContribution<?>> commentNode = iterator.next();
            PublicContribution<?> publicContribution = commentNode.getSubject();

            if (publicContribution instanceof Comment) {
                Comment comment = (Comment) publicContribution;
                Document commentDoc = new Document();
                commentDoc.append("id", comment.getId())
                        .append("author", comment.getAuthor())
                        .append("body", comment.getBody())
                        .append("score", comment.getScore())
                        .append("created", comment.getCreated().getTime());
                comments.add(commentDoc);
            }
        }

        return comments;
    }

}
