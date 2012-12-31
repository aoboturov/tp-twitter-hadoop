package com.oboturov.ht.jobs;

import com.oboturov.ht.Nuplet;
import com.oboturov.ht.Tweet;
import com.oboturov.ht.User;
import com.oboturov.ht.etl.NupletCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Script generates Nuplets from raw Tweet data and saves each Nuplet represented as JSON string to the output file.
 * Script accepts two parameters:
 * <ol>
 *   <li>comma-separated list of input files</li>
 *   <li>output file name</li>
 * </ol>
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
    public int run(final String[] scriptArgs) throws Exception {
        final GenericOptionsParser optionsParser = new GenericOptionsParser(scriptArgs);

        final String[] args = optionsParser.getRemainingArgs();
        final Configuration config = optionsParser.getConfiguration();

        final JobConf jobConf = new JobConf(config, NupletsGenerator.class);
        jobConf.setJobName("nuplets-generator");

        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Nuplet.class);

//        jobConf.setOutputKeyComparatorClass(UserComparator.class);
//        jobConf.setOutputValueGroupingComparator(NupletComparator.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, args[0]);
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        // Extract tweets
//        ChainMapper.addMapper(
//                jobConf,
//                TweetsReader.Map.class,
//                LongWritable.class,
//                Text.class,
//                LongWritable.class,
//                Tweet.class,
//                false,
//                new JobConf(false)
//        );
//        // Map tweets to count measure.
//        ChainMapper.addMapper(
//                jobConf,
//                NupletCreator.Map.class,
//                LongWritable.class,
//                Tweet.class,
//                NullWritable.class,
//                Nuplet.class,
//                true,
//                new JobConf(false)
//        );

        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(final String args[]) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new NupletsGenerator(), args);

        System.exit(res);
    }
}
