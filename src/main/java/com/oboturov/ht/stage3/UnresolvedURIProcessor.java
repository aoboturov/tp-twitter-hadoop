package com.oboturov.ht.stage3;

import com.oboturov.ht.ConfigUtils;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.stage2.NupletsReader;
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
 * @author aoboturov
 */
public class UnresolvedURIProcessor extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {
        final JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("uri-resolver");

        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        ConfigUtils.makeMapOutputCompressedWithBZip2(conf);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Read raw nuplets.
        ChainMapper.addMapper(
                conf,
                NupletsReader.Map.class,
                LongWritable.class,
                Text.class,
                NullWritable.class,
                Nuplet.class,
                true,
                new JobConf(false)
        );
        // Resolve URIs
        ChainMapper.addMapper(
                conf,
                URIResolver.Map.class,
                NullWritable.class,
                Nuplet.class,
                NullWritable.class,
                Nuplet.class,
                true,
                new JobConf(false)
        );

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new UnresolvedURIProcessor(), args);

        System.exit(res);
    }
}
