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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.bson.Document;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.net.HttpURLConnection;
import java.net.URL;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.json.JSONObject;
import org.json.JSONArray;

public class PostsToKafka {

    // Reddit API credentials
    private static final String clientId = "<REDDIT_CLIENT_ID>";
    private static final String clientSecret = "<REDDIT_CLIENT_SECRET>";
    private static final String username = "<REDDIT_USERNAME>";
    private static final String password = "<REDDIT_PASSWORD>";

    private static final Set<String> seenPosts = new HashSet<>();
    private static final Set<String> seenRedditPosts = new HashSet<>();

    public static void main(String[] args) {


        // Set up JRAW with your Reddit API credentials
        UserAgent userAgent = new UserAgent("bot", "com.example.reddit", "v0.1", "your_reddit_username");
        Credentials credentials = Credentials.script(username, password, clientId, clientSecret);
        RedditClient redditClient = OAuthHelper.automatic(new net.dean.jraw.http.OkHttpNetworkAdapter(userAgent), credentials);

        // Kafka configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", "<KAFKA-CLUSTER-ID>:9092"); // replace <cluster-id> with your Confluent Cloud cluster ID
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='<KAFKA API KEY>' password='<KAFKA-API-SECRET>';"); // replace <api-key> and <api-secret> with your Confluent Cloud API key and secret
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
                fetchStackOverflowPosts(kafkaProducer);
            }
        }, 0, 60 * 1000);
    }

    public static void fetchNewPostsAndComments(RedditClient redditClient, String subredditName, Producer<String, byte[]> kafkaProducer) {
        // Fetch new posts from the specified subreddit
        DefaultPaginator<Submission> paginator = redditClient.subreddit(subredditName).posts().sorting(SubredditSort.NEW).limit(50).build();

        // Iterate through the fetched posts and store them in the Kafka topic along with their comments
        for (Submission post : paginator.next()) {
            // Only process this post if we haven't seen it before
            String postId = post.getId();
            if (!seenRedditPosts.contains(postId)) {
                seenRedditPosts.add(postId);

                // Create a unique identifier for the post
                String postHash = Hashing.sha256().hashString(post.getTitle() + post.getAuthor(), StandardCharsets.UTF_8).toString();

                // Create a new BSON document for the post and its comments
                Document postDoc = new Document();
                postDoc.append("id", postId);
                postDoc.append("title", post.getTitle());
                postDoc.append("author", post.getAuthor());
                postDoc.append("score", post.getScore());
                postDoc.append("permalink", post.getPermalink());
                postDoc.append("url", post.getUrl());
                postDoc.append("description", post.getSelfText());
                postDoc.append("created", new Date(post.getCreated().getTime()));

                List<Document> comments = new ArrayList<>();
                for (Document comment : getCommentsAsDocuments(redditClient, postId)) {
                    comments.add(comment);
                }
                postDoc.append("comments", comments);

                // Convert Document to bytes
                byte[] postDocBytes = postDoc.toJson().getBytes(StandardCharsets.UTF_8);

                // Send the BSON document to Kafka
                kafkaProducer.send(new ProducerRecord<>("topic_3", postHash, postDocBytes));

                System.out.println("Stored post and comments: " + post.getTitle());
            }
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
    public static void fetchStackOverflowPosts(Producer<String, byte[]> kafkaProducer) {
        String[] tags = new String[]{"mongodb"}; // Define the tags you want to fetch posts about


        for (String tag : tags) {
            try {
                // Define the endpoint for fetching questions with the specified tag
                String endpoint = "https://api.stackexchange.com/2.3/questions?order=desc&sort=activity&tagged=" + tag + "&site=stackoverflow";

                // Connect to the endpoint and get the JSON response
                URL url = new URL(endpoint);
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");

                // Add the necessary headers
                connection.setRequestProperty("Accept-Charset", "UTF-8");
                connection.setRequestProperty("Accept-Encoding", "gzip");

                // Ensure the connection is successful
                int status = connection.getResponseCode();
                if (status != 200) {
                    throw new RuntimeException("Failed to get posts: HTTP error code : " + connection.getResponseCode());
                }

                // Decode the gzip-encoded JSON response
                InputStream inStream = new GZIPInputStream(connection.getInputStream());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inStream));
                String line;
                StringBuilder response = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }

                // Parse the JSON response
                JSONObject jsonObject = new JSONObject(response.toString());
                JSONArray items = jsonObject.getJSONArray("items");

                // Iterate through the fetched posts and store them in the Kafka topic
                for (int i = 0; i < items.length(); i++) {
                    JSONObject post = items.getJSONObject(i);

                    String postId = Integer.toString(post.getInt("question_id"));

                    // Only process this post if we haven't seen it before
                    if (!seenPosts.contains(postId)) {
                        seenPosts.add(postId);

                        // Create a unique identifier for the post
                        String postHash = Hashing.sha256().hashString(post.getString("title") + post.getJSONObject("owner").getString("display_name"), StandardCharsets.UTF_8).toString();

                        // Create a new BSON document for the post
                        Document postDoc = new Document();
                        postDoc.append("id", postId);
                        postDoc.append("title", post.getString("title"));
                        postDoc.append("link", post.getString("link"));
                        postDoc.append("created", new Date(post.getInt("creation_date") * 1000L)); // Convert UNIX timestamp to Date

                        // Convert Document to bytes
                        byte[] postDocBytes = postDoc.toJson().getBytes(StandardCharsets.UTF_8);

                        // Send the BSON document to Kafka
                        kafkaProducer.send(new ProducerRecord<>("topic_3", postHash, postDocBytes));

                        System.out.println("Stored post: " + post.getString("title"));
                    }
                }

                // Close the connection
                connection.disconnect();

            } catch (RuntimeException | IOException e) {
                System.out.println("Failed to get posts: " + e.getMessage());
            }
        }
    }


}

