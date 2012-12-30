package com.oboturov.ht.jobs;

import com.oboturov.ht.Tweet;
import com.oboturov.ht.etl.TweetsReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * Class produces Key-Value ordered by Key map of twitter user names with a number of tweets issued by user.
 * Script accepts two parameters:
 * <ol>
 *   <li>comma-separated list of input files</li>
 *   <li>output file name</li>
 * </ol>
 * @author aoboturov
 */
public class UsersCounter extends Configured implements Tool {

    private static class UserMap extends MapReduceBase implements Mapper<LongWritable, Tweet, Text, LongWritable> {
        @Override
        public void map(final LongWritable key, final Tweet tweet, final OutputCollector< Text, LongWritable> output, final Reporter reporter) throws IOException {
            output.collect(new Text(tweet.getUser().getName()),  new LongWritable(1L));
        }
    }

    @Override
    public int run(final String[] scriptArgs) throws Exception {
        final GenericOptionsParser optionsParser = new GenericOptionsParser(scriptArgs);

        final String[] args = optionsParser.getRemainingArgs();
        final Configuration config = optionsParser.getConfiguration();

        // Set up first job reading tweets and mapping them to users.
        final JobConf conf = new JobConf(config, UsersCounter.class);
        conf.setJobName("user-twitted-cnt-generator");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setMapperClass(TweetsReader.Map.class);
        conf.setReducerClass(LongSumReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

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
        // Map tweets to users who produced them.
        ChainMapper.addMapper(
                conf,
                UserMap.class,
                LongWritable.class,
                Tweet.class,
                Text.class,
                LongWritable.class,
                true,
                new JobConf(false)
        );

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new UsersCounter(), args);

        System.exit(res);
    }
}
