package com.oboturov.ht.stage1;

import com.oboturov.ht.Tweet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Class counts the number of tweets contained in source file and produce a single numerical counter value.
 * Script accepts two parameters:
 * <ol>
 *   <li>comma-separated list of input files</li>
 *   <li>output file name</li>
 * </ol>
 * @author aoboturov
 * @see com.oboturov.ht.stage0.TweetsReader.Map.Counters#TWEETS_READ
 */
public class TweetsCounter extends Configured implements Tool {

    private static class CountMeasureMap extends MapReduceBase implements Mapper<NullWritable, Tweet, NullWritable, LongWritable> {
        private final LongWritable one = new LongWritable(1L);

        @Override
        public void map(final NullWritable key, final Tweet tweet, final OutputCollector<NullWritable, LongWritable> outputCollector, final Reporter reporter) throws IOException {
            outputCollector.collect(NullWritable.get(), one);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("tweets-count");

        conf.setNumMapTasks(10);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setCombinerClass(LongSumReducer.class);
        conf.setReducerClass(LongSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Extract tweets
        ChainMapper.addMapper(
                conf,
                GeneratedTweetsReader.Map.class,
                LongWritable.class,
                Text.class,
                NullWritable.class,
                Tweet.class,
                false,
                new JobConf(false)
        );
        // Map tweets to count measure.
        ChainMapper.addMapper(
                conf,
                CountMeasureMap.class,
                NullWritable.class,
                Tweet.class,
                NullWritable.class,
                LongWritable.class,
                true,
                new JobConf(false)
        );

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new TweetsCounter(), args);

        System.exit(res);
    }
}
