package com.oboturov.ht.stage1;

import com.oboturov.ht.Nuplet;
import com.oboturov.ht.Tweet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * The Script generates Nuplets from Generated Tweet data and saves each Nuplet represented as JSON string to the output file.
 * Script accepts two parameters:
 * <ol>
 *   <li>comma-separated list of input files</li>
 *   <li>output file name</li>
 * </ol>
 * @author aoboturov
 */
public class NupletsGenerator extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {
        final JobConf jobConf = new JobConf(getConf(), getClass());

        jobConf.setJobName("nuplets-generator");

        jobConf.setOutputKeyClass(NullWritable.class);
        jobConf.setOutputValueClass(Nuplet.class);

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(jobConf, args[0]);
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        // Read generated tweets.
        ChainMapper.addMapper(
                jobConf,
                GeneratedTweetsReader.Map.class,
                LongWritable.class,
                Text.class,
                NullWritable.class,
                Tweet.class,
                false,
                new JobConf(false)
        );
        // Generate nuplets.
        ChainMapper.addMapper(
                jobConf,
                NupletCreator.Map.class,
                NullWritable.class,
                Tweet.class,
                NullWritable.class,
                Nuplet.class,
                true,
                new JobConf(false)
        );

        JobClient.runJob(jobConf);

        return 0;
    }

    public static void main(final String args[]) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new NupletsGenerator(), args);

        System.exit(res);
    }
}
