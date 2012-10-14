package com.oboturov.ht.jobs;

import com.oboturov.ht.Nuplet;
import com.oboturov.ht.Tweet;
import com.oboturov.ht.User;
import com.oboturov.ht.etl.NupletCreator;
import com.oboturov.ht.etl.TweetsReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;

/**
 * @author aoboturov
 */
public class NupletsGenerator {

    public static void main(String args[]) throws Exception {
        final JobConf conf = new JobConf();
        conf.setJobName("nuplets-generator");

        conf.setOutputKeyClass(User.class);
        conf.setOutputValueClass(Nuplet.class);

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
                NupletCreator.Map.class,
                LongWritable.class,
                Tweet.class,
                User.class,
                Nuplet.class,
                true,
                new JobConf(false)
        );

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
