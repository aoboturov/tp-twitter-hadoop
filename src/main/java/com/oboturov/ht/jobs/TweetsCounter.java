package com.oboturov.ht.jobs;

import com.oboturov.ht.Tweet;
import com.oboturov.ht.etl.TweetsReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class TweetsCounter {

    private static final boolean TWEETS_COUNTER_KEY = true;

    private static class CountMeasureMap extends MapReduceBase implements Mapper<LongWritable, Tweet, BooleanWritable, LongWritable> {
        @Override
        public void map(final LongWritable key, final Tweet tweet, final OutputCollector<BooleanWritable, LongWritable> outputCollector, final Reporter reporter) throws IOException {
            outputCollector.collect(new BooleanWritable(TWEETS_COUNTER_KEY), new LongWritable(1L));
        }
    }

    public static void main(String[] args) throws Exception {
        final JobConf conf = new JobConf(TweetsCounter.class);
        conf.setJobName("tweets-count");

        conf.setOutputKeyClass(BooleanWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setCombinerClass(LongSumReducer.class);
        conf.setReducerClass(LongSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        // Extract tweets
        ChainMapper.addMapper(
                conf,
                TweetsReader.Map.class,
                LongWritable.class,
                Text.class,
                LongWritable.class,
                Tweet.class,
                false,
                new JobConf(false)
        );
        // Map tweets to count measure.
        ChainMapper.addMapper(
                conf,
                CountMeasureMap.class,
                LongWritable.class,
                Tweet.class,
                BooleanWritable.class,
                LongWritable.class,
                true,
                new JobConf(false)
        );

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

}
