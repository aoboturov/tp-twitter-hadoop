package com.oboturov.ht.stage1;

import com.oboturov.ht.ConfigUtils;
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
 * Class produces Key-Value ordered by Key map of twitter user names with a number of tweets issued by user.
 * Script accepts two parameters:
 * <ol>
 *   <li>comma-separated list of input files</li>
 *   <li>output file name</li>
 * </ol>
 * @author aoboturov
 */
public class UsersListGenerator extends Configured implements Tool {

    private static class UserMap extends MapReduceBase implements Mapper<NullWritable, Tweet, Text, LongWritable> {
        private final LongWritable one = new LongWritable(1L);
        private final Text userName = new Text();

        @Override
        public void map(final NullWritable key, final Tweet tweet, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            userName.set(tweet.getUser().getName());
            output.collect(userName, one);
        }
    }

    @Override
    public int run(final String[] scriptArgs) throws Exception {
        final GenericOptionsParser optionsParser = new GenericOptionsParser(scriptArgs);

        final String[] args = optionsParser.getRemainingArgs();
        final Configuration config = optionsParser.getConfiguration();

        // Set up first job reading tweets and mapping them to users.
        final JobConf conf = new JobConf(config, UsersListGenerator.class);
        conf.setJobName("users-list-generator");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        ConfigUtils.makeMapOutputCompressedWithBZip2(conf);

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
        // Map tweets to users who produced them and a counter of tweets per user.
        final JobConf userMapConf = new JobConf(false);
        userMapConf.setReducerClass(LongSumReducer.class);
        ChainMapper.addMapper(
                conf,
                UserMap.class,
                NullWritable.class,
                Tweet.class,
                Text.class,
                LongWritable.class,
                true,
                userMapConf
        );

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new UsersListGenerator(), args);

        System.exit(res);
    }
}
