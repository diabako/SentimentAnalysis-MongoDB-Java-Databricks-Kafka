package com.mongodb.sentiment;
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
import org.bson.Document;
import java.time.Instant;
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
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='RP7QF5PFXXVPARTC' password='YXXUl+ia6nrxN9H75Cr5A2w9fuGMPccQlYyRskzb0T+4fEtlp4RBjoA0Y365HE1G';"); // replace <api-key> and <api-secret> with your Confluent Cloud API key and secret
        props.put("sasl.mechanism", "PLAIN");
        props.put("ssl.endpoint.identification.algorithm", "https");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        Producer<String, byte[]> kafkaProducer = new KafkaProducer<>(props);
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

    public static void fetchNewPostsAndComments(RedditClient redditClient, String subredditName, Producer<String, byte[]> kafkaProducer) {
        // Fetch new posts from the specified subreddit
        DefaultPaginator<Submission> paginator = redditClient.subreddit(subredditName).posts().sorting(SubredditSort.NEW).limit(50).build();

        // Iterate through the fetched posts and store them in the Kafka topic along with their comments
        for (Submission post : paginator.next()) {
            // Create a unique identifier for the post
            String postHash = Hashing.sha256().hashString(post.getTitle() + post.getAuthor(), StandardCharsets.UTF_8).toString();

            // Create a new BSON document for the post and its comments
            Document postDoc = new Document();
            postDoc.append("id", post.getId());
            postDoc.append("title", post.getTitle());
            postDoc.append("author", post.getAuthor());
            postDoc.append("score", post.getScore());
            postDoc.append("permalink", post.getPermalink());
            postDoc.append("url", post.getUrl());
            postDoc.append("description", post.getSelfText());
            postDoc.append("created", Date.from(Instant.ofEpochSecond(post.getCreated().getSeconds())));

            List<Document> comments = new ArrayList<>();
            for (Document comment : getCommentsAsDocuments(redditClient, post.getId())) {
                comments.add(comment);
            }
            postDoc.append("comments", comments);

            // Convert Document to bytes
            byte[] postDocBytes = postDoc.toJson().getBytes(StandardCharsets.UTF_8);

            // Send the BSON document to Kafka
            kafkaProducer.send(new ProducerRecord<>("topic_5", postHash, postDocBytes));

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
                        .append("created", new Date(comment.getCreated().getTime()));
                comments.add(commentDoc);
            }
        }

        return comments;
    }
}
