package com.oboturov.ht.stage1;

import com.oboturov.ht.ObjectMapperInstance;
import com.oboturov.ht.Tweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class GeneratedTweetsReader {

    private final static Logger logger = Logger.getLogger(GeneratedTweetsReader.class);

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, Tweet> {

        enum Counters {
            GENERATED_TWEET_DESERIALIZATION_ERROR
        }

        @Override
        public void map(final LongWritable key, final Text value, final OutputCollector<NullWritable, Tweet> output, final Reporter reporter) throws IOException {
            try {
                final String tweetSerializedToJson = value.toString();
                final Tweet tweet = ObjectMapperInstance.get().readValue(tweetSerializedToJson, Tweet.class);
                output.collect(NullWritable.get(), tweet);
            } catch (IOException e) {
                logger.error("Unable to read a generated Tweet", e);
                reporter.incrCounter(Counters.GENERATED_TWEET_DESERIALIZATION_ERROR, 1l);
            }
        }
    }

}
