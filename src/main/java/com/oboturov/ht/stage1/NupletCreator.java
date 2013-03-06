package com.oboturov.ht.stage1;

import com.oboturov.ht.*;
import com.oboturov.ht.pig.TweetEntityExtractor;
import com.twitter.Extractor;
import org.apache.hadoop.io.NullWritable;
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

    public static class Map extends MapReduceBase implements Mapper<NullWritable, Tweet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(Map.class);

        enum Counters {
            SKIPPED_CASHTAG_NUPLET, ILLEGAL_TWEET_ENTITY_TYPE, NUPLETS_WITH_ITEMS_GENERATED,
            NUPLETS_WITH_ONLY_KEYWORDS_GENERATED
        }

        private final ThreadLocal<Extractor> extractor = new ThreadLocal<Extractor>() {
            @Override
            protected Extractor initialValue() {
                return new Extractor();
            }
        };

        @Override
        public void map(final NullWritable offset, final Tweet tweet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            final String text = tweet.getPost();
            // Handle hashes.
            final List<Extractor.Entity> entities = extractor.get().extractEntitiesWithIndices(text);
            final String rawText = TweetEntityExtractor.extractRawTextFromTweetPost(text, entities);

            final User user = tweet.getUser();

            // Generate one nuplet per Item.
            for (Extractor.Entity entity : entities) {
                String type;
                switch (entity.getType()) {
                    case CASHTAG:
                        // Do not support CASHTAGs in analysis phase.
                        reporter.incrCounter(Counters.SKIPPED_CASHTAG_NUPLET, 1l);
                        continue;
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
                        logger.error(String.format("No such type of Tweeter entity: %s", entity.getType().name()));
                        reporter.incrCounter(Counters.ILLEGAL_TWEET_ENTITY_TYPE, 1l);
                        continue;
                }
                final Item item = new Item(type, entity.getValue());
                final Nuplet nuplet = new Nuplet();
                nuplet.setUser(user);
                nuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, rawText));
                nuplet.setItem(item);
                output.collect(NullWritable.get(), nuplet);
                reporter.incrCounter(Counters.NUPLETS_WITH_ITEMS_GENERATED, 1l);
            }

            // Generate a nuplet with no Item.
            final Nuplet nuplet = new Nuplet();
            nuplet.setUser(user);
            nuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, rawText));
            nuplet.setItem(Item.NULL);
            output.collect(NullWritable.get(), nuplet);
            reporter.incrCounter(Counters.NUPLETS_WITH_ONLY_KEYWORDS_GENERATED, 1l);
        }
    }

}
