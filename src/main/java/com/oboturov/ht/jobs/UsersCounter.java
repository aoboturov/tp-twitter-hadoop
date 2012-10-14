package com.oboturov.ht.jobs;

import com.oboturov.ht.Tweet;
import com.oboturov.ht.etl.TweetsReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.LongSumReducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author aoboturov
 */
public class UsersCounter {

    private static class UserMap extends MapReduceBase implements Mapper<LongWritable, Tweet, Text, LongWritable> {
        @Override
        public void map(final LongWritable key, final Tweet tweet, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            output.collect(new Text(tweet.getUser().getName()), new LongWritable(1L));
        }
    }

    private static class DuplicateUserEliminationReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(final Text key, final Iterator<LongWritable> values, final OutputCollector<Text, LongWritable> output, final Reporter reporter) throws IOException {
            if ( values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    private static class UserCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, BooleanWritable, LongWritable> {
        @Override
        public void map(final LongWritable key, final Text value, final OutputCollector<BooleanWritable, LongWritable> output, final Reporter reporter) throws IOException {
            output.collect(new BooleanWritable(true), new LongWritable(1L));
        }
    }

    public static void main(String[] args) throws Exception {

        // Set up first job reading tweets and mapping them to users.
        final JobConf tweetsReadConfig = new JobConf();
        tweetsReadConfig.setJobName("tweets-read");

        tweetsReadConfig.setOutputKeyClass(Text.class);
        tweetsReadConfig.setOutputValueClass(LongWritable.class);

        tweetsReadConfig.setMapperClass(TweetsReader.Map.class);
        tweetsReadConfig.setReducerClass(DuplicateUserEliminationReducer.class);

        tweetsReadConfig.setInputFormat(TextInputFormat.class);
        tweetsReadConfig.setOutputFormat(TextOutputFormat.class);

        // Extract tweets
        ChainMapper.addMapper(
                tweetsReadConfig,
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
                tweetsReadConfig,
                UserMap.class,
                LongWritable.class,
                Tweet.class,
                Text.class,
                LongWritable.class,
                true,
                new JobConf(false)
        );

        FileInputFormat.setInputPaths(tweetsReadConfig, new Path(args[0]));
        FileOutputFormat.setOutputPath(tweetsReadConfig, new Path("users-list"));

        final Job tweetsReadJob = new Job(tweetsReadConfig);

        // Second Job making accumulation of total users number.
        final JobConf usersCountConfig = new JobConf();
        usersCountConfig.setJobName("users-count");

        usersCountConfig.setOutputKeyClass(BooleanWritable.class);
        usersCountConfig.setOutputValueClass(LongWritable.class);

        usersCountConfig.setMapperClass(UserCountMapper.class);
        usersCountConfig.setReducerClass(LongSumReducer.class);

        usersCountConfig.setInputFormat(TextInputFormat.class);
        usersCountConfig.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(usersCountConfig, new Path("users-list"));
        FileOutputFormat.setOutputPath(usersCountConfig, new Path(args[1]));

        final Job usersCountJob = new Job(usersCountConfig, new ArrayList<Job>(Arrays.asList(tweetsReadJob)));

        // Launch jobs cascade.
        final JobControl jobControl = new JobControl("users-counter-job-control");
        jobControl.addJob(usersCountJob);

        jobControl.run();
    }

}
