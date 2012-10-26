package com.oboturov.ht.jobs;

import com.oboturov.ht.Nuplet;
import com.oboturov.ht.Tweet;
import com.oboturov.ht.User;
import com.oboturov.ht.etl.NupletCreator;
import com.oboturov.ht.etl.TweetsReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author aoboturov
 */
public class NupletsGenerator extends Configured implements Tool {

    static class UserComparator extends WritableComparator {
        public UserComparator() {
            super(User.class, true);
        }
    }

    static class NupletComparator extends WritableComparator {
        public NupletComparator() {
            super(Nuplet.class, true);
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration config = getConf();

        final JobConf jobConf = new JobConf(config, NupletsGenerator.class);
        jobConf.setJobName("nuplets-generator");

        jobConf.setOutputKeyClass(User.class);
        jobConf.setOutputValueClass(Nuplet.class);
        jobConf.setOutputKeyComparatorClass(UserComparator.class);
        jobConf.setOutputValueGroupingComparator(NupletComparator.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        // Extract tweets
        ChainMapper.addMapper(
                jobConf,
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
                jobConf,
                NupletCreator.Map.class,
                LongWritable.class,
                Tweet.class,
                User.class,
                Nuplet.class,
                true,
                new JobConf(false)
        );

        FileInputFormat.setInputPaths(jobConf, new Path(args[args.length-2]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[args.length-1]));

        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(final String args[]) throws Exception {
        for (String arg: args) {
            System.out.println(arg);
        }

        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new NupletsGenerator(), args);

        System.exit(res);
    }
}
