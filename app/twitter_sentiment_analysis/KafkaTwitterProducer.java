package twitter_sentiment_analysis;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.kafka.clients.producer.*;

public class KafkaTwitterProducer {
	public static void main(String[] args) throws Exception{
		LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
		if (args.length < 5) {
			System.out.println("KafkaTwitterProducer  <twitter-consumer-key> <twitter-consumer-secret>"
					+ " <twitter-access-token>"
					+ "<twitter-access-token-secret>"
					+ "<topic-name> <twitter-search-keywords>");
            return;
		}
		String consumerKey = args[0];
		String consumerSecret = args[1];
		String accessToken = args[2];
		String accessTokenSecret = args[3];
		String topicName = args[4];
		//String[] arguments = args.clone();
		//String[] keyWords = Arrays.copyOfRange(arguments, 5, arguments.length);
		

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true)
		.setOAuthConsumerKey(consumerKey)
		.setOAuthConsumerSecret(consumerSecret)
		.setOAuthAccessToken(accessToken)
		.setOAuthAccessTokenSecret(accessTokenSecret);
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				queue.offer(status);
			}
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            
         	}
         	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

         	}
         	public void onScrubGeo(long userId, long upToStatusId) {
            
         	}
         	public void onStallWarning(StallWarning warning) {
            
         	}      
         	public void onException(Exception ex) {
            	ex.printStackTrace();
         	}
		};
		twitterStream.addListener(listener);
		Thread.sleep(1000);
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);

		props.put("key.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      	props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");

      	Producer<String, String> producer = new KafkaProducer<String, String>(props);
      	int i = 0;
      	int j = 0;
      	while (i < 10) {
      		Status ret = queue.poll();
      		if (ret == null) {
      			Thread.sleep(100);
      			i++;
      		} else {
      			for (HashtagEntity hashtag : ret.getHashtagEntities()) {
      				System.out.println("HashTag:" + hashtag.getText());
      				producer.send(new ProducerRecord<String, String>(topicName, 
      					Integer.toString(j++), hashtag.getText()));
      			}
      		}

      	}
      	producer.close();
      	Thread.sleep(5000);
      	twitterStream.shutdown();
	}

}

