package com.oboturov.ht.stage2;

import com.oboturov.ht.ItemType;
import com.oboturov.ht.KeyType;
import com.oboturov.ht.Nuplet;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class NupletsWithKeywordsProcessedSplitter extends Configured implements Tool {

    public static final String NUPLETS_WITH_NO_ITEMS_TXT = "nupletswithnoitems";
    public static final String NUPLETS_REQUIRING_URIS_RESOLUTION_TXT = "nupletsrequiringurisresolution";
    public static final String NUPLETS_WITH_NO_KEYWORDS_AND_NO_URIS_TXT = "nupletswithnokeywordsandnouris";
    public static final String NUPLETS_WITH_KEYWORDS_AND_NO_URIS_TXT = "nupletswithkeywordsandnouris";

    static class KeywordSplitterMapper extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(NupletsWithKeywordsProcessedSplitter.class);

        private MultipleOutputs multipleOutputs;

        enum Counters {
            NUPLETS_WITH_NO_ITEMS, NUPLETS_REQUIRING_URL_RESOLUTION, NUPLETS_WITH_NO_KEYWORDS_AND_NO_URIS,
            NUPLETS_WITH_KEYWORDS_AND_NO_URIS
        }

        @Override
        public void configure(final JobConf conf) {
            TextOutputFormat.setOutputCompressorClass(conf, BZip2Codec.class);
            multipleOutputs = new MultipleOutputs(conf);
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, Reporter reporter) throws IOException {
            final String nupletItemType = nuplet.getItem().getType();

            String namedOutput = null;
            Counters counter = null;

            // Nuplets with keywords only.
            if (ItemType.NULL.equals(nupletItemType)) {
                namedOutput = NUPLETS_WITH_NO_ITEMS_TXT;
                counter = Counters.NUPLETS_WITH_NO_ITEMS;
            }

            // Nuplets with items.
            if (ItemType.URL.equals(nupletItemType)) {
                namedOutput = NUPLETS_REQUIRING_URIS_RESOLUTION_TXT;
                counter = Counters.NUPLETS_REQUIRING_URL_RESOLUTION;
            }

            if (KeyType.NULL.equals(nuplet.getKeyword().getType())) {
                namedOutput = NUPLETS_WITH_NO_KEYWORDS_AND_NO_URIS_TXT;
                counter = Counters.NUPLETS_WITH_NO_KEYWORDS_AND_NO_URIS;
            }

            if (namedOutput == null) {
                namedOutput = NUPLETS_WITH_KEYWORDS_AND_NO_URIS_TXT;
                counter = Counters.NUPLETS_WITH_KEYWORDS_AND_NO_URIS;
            }

            final OutputCollector collector = multipleOutputs.getCollector(namedOutput, reporter);
            collector.collect(nothing, nuplet);
            reporter.incrCounter(counter, 1l);
        }

        @Override
        public void close() throws IOException {
            multipleOutputs.close();
        }
    }

    @Override
    public int run(final String[] args) throws Exception {
        final JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("nuplets-by-keyword-splitter");

        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);

        FileInputFormat.setInputPaths(conf, args[0]);
        MultipleOutputFormat.setOutputPath(conf, new Path(args[1]));
        MultipleOutputFormat.setCompressOutput(conf, true);
        MultipleOutputFormat.setOutputCompressorClass(conf, BZip2Codec.class);

        MultipleOutputs.addNamedOutput(conf, NUPLETS_WITH_NO_ITEMS_TXT, TextOutputFormat.class, NullWritable.class, Nuplet.class);
        MultipleOutputs.addNamedOutput(conf, NUPLETS_REQUIRING_URIS_RESOLUTION_TXT, TextOutputFormat.class, NullWritable.class, Nuplet.class);
        MultipleOutputs.addNamedOutput(conf, NUPLETS_WITH_NO_KEYWORDS_AND_NO_URIS_TXT, TextOutputFormat.class, NullWritable.class, Nuplet.class);
        MultipleOutputs.addNamedOutput(conf, NUPLETS_WITH_KEYWORDS_AND_NO_URIS_TXT, TextOutputFormat.class, NullWritable.class, Nuplet.class);

        ChainMapper.addMapper(
                conf,
                NupletsReader.Map.class,
                LongWritable.class,
                Text.class,
                NullWritable.class,
                Nuplet.class,
                true,
                new JobConf(true)
        );
        ChainMapper.addMapper(
                conf,
                KeywordSplitterMapper.class,
                NullWritable.class,
                Nuplet.class,
                NullWritable.class,
                Nuplet.class,
                true,
                new JobConf(true)
        );

        JobClient.runJob(conf);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new NupletsWithKeywordsProcessedSplitter(), args);

        System.exit(res);
    }
}
