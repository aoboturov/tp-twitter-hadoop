package com.oboturov.ht.etl;

import com.oboturov.ht.*;
import com.twitter.Extractor;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author aoboturov
 */
public class NupletCreator {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Tweet, User, Nuplet> {

        private final static Logger logger = Logger.getLogger(Map.class);

        private final Extractor extractor = new Extractor();

        private String extractRawTextFromTweetPost(final String post, final List<Extractor.Entity> entities) {
            int left = 0;
            final StringBuilder rawText = new StringBuilder(post.length());
            for (Extractor.Entity entity : entities) {
                rawText.append(post.substring(left, entity.getStart()));
                left = entity.getEnd();
            }
            rawText.append(post.substring(left, post.length()));
            return rawText.toString();
        }

        @Override
        public void map(final LongWritable key, final Tweet tweet, final OutputCollector<User, Nuplet> output, final Reporter reporter) throws IOException {
            String text = tweet.getPost();
            // Handle hashes.
            final List<Extractor.Entity> entities = extractor.extractEntitiesWithIndices(text);
            final String rawText = extractRawTextFromTweetPost(text, entities);

            final User user = tweet.getUser();
            for (Extractor.Entity entity : entities) {
                String type;
                switch (entity.getType()) {
                    case CASHTAG:
                        type = ItemType.CASH;
                        break;
                    case HASHTAG:
                        type = ItemType.HASH;
                        break;
                    case MENTION:
                        type = ItemType.AT;
                        break;
                    case URL:
                        type = ItemType.URL;
                        break;
                    default:
                        logger.error("No such type of Tweeter entity: "+entity.getType().name());
                        continue;
                }
                final Item item = new Item(type, entity.getValue());
                final Nuplet nuplet = new Nuplet();
                nuplet.setUser(user);
                nuplet.setKeyword(new Keyword(KeyType.PLAIN_TEXT, rawText));
                nuplet.setItem(item);
                output.collect(user, nuplet);
            }
        }
    }

}
