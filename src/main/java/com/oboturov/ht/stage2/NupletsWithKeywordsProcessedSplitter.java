package com.oboturov.ht.stage2;

import com.oboturov.ht.ConfigUtils;
import com.oboturov.ht.ItemType;
import com.oboturov.ht.KeyType;
import com.oboturov.ht.Nuplet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author aoboturov
 */
public class NupletsWithKeywordsProcessedSplitter extends Configured implements Tool {

    static class KeywordSplitterMultipleTextOutputFormat extends MultipleTextOutputFormat<NullWritable, Nuplet> {
        @Override
        protected String generateFileNameForKeyValue(final NullWritable nothing, final Nuplet nuplet, String name) {
            final String nupletItemType = nuplet.getItem().getType();

            // Nuplets with keywords only.
            if (ItemType.NULL.equals(nupletItemType)) {
                return "nuplets-with-no-items";
            }

            // Nuplets with items.
            if (ItemType.URL.equals(nupletItemType)) {
                return "nuplets-requiring-uris-resolution";
            }

            if (KeyType.NULL.equals(nuplet.getKeyword().getType())) {
                return "nuplets-with-no-keywords-and-no-uris";
            }
            return "nuplets-with-keywords-and-no-uris";
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("nuplets-by-keyword-splitter");

        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(KeywordSplitterMultipleTextOutputFormat.class);

        conf.setMapperClass(NupletsReader.Map.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Nuplet.class);
        ConfigUtils.makeMapOutputCompressedWithBZip2(conf);

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new NupletsWithKeywordsProcessedSplitter(), args);

        System.exit(res);
    }
}
