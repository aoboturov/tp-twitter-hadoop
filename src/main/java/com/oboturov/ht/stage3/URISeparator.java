package com.oboturov.ht.stage3;

import com.oboturov.ht.Item;
import com.oboturov.ht.ItemType;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.pig.InvalidUrlRemover;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author aoboturov
 */
public class URISeparator {
    public static class Map extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {
        private final static Logger logger = Logger.getLogger(URISeparator.class);

        public static final String SHORTENED_URIS_FILE = "shorteneduris";
        public static final String FULL_URIS_FILE = "fulluris";

        private MultipleOutputs multipleOutputs;

        enum Counters {
            SHORTENED_URIS, FULL_URIS, URIS_TO_DEAD_SHORTENERS, ILLEGAL_URIS
        }

        @Override
        public void configure(final JobConf conf) {
            TextOutputFormat.setOutputCompressorClass(conf, BZip2Codec.class);
            multipleOutputs = new MultipleOutputs(conf);
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (ItemType.URL.equals(nuplet.getItem().getType())) {
                try {
                    String link = nuplet.getItem().getValue();
                    if (!link.startsWith("http://") && !link.startsWith("https://")) {
                        link = "http://"+link;
                        // Update URL value.
                        nuplet.setItem(new Item(ItemType.URL, link));
                    }
                    final URL url = new URL(link);
                    if (InvalidUrlRemover.isDeadShortenerUrl(url)) {// Discard those shortened URLs which could not be resolved.
                        reporter.incrCounter(Counters.URIS_TO_DEAD_SHORTENERS, 1l);
                        return;
                    } else if (InvalidUrlRemover.isValidShortenerUrl(url)) {// Handle redirect to full URL.
                        reporter.incrCounter(Counters.SHORTENED_URIS, 1l);
                        multipleOutputs.getCollector(SHORTENED_URIS_FILE, reporter).collect(nothing, nuplet);
                        return;
                    } else {
                        multipleOutputs.getCollector(FULL_URIS_FILE, reporter).collect(nothing, nuplet);
                        reporter.incrCounter(Counters.FULL_URIS, 1l);
                    }
                } catch (MalformedURLException e) {
                    logger.error("Illegal URL", e);
                    reporter.incrCounter(Counters.ILLEGAL_URIS, 1l);
                }
            }
        }

        @Override
        public void close() throws IOException {
            multipleOutputs.close();
        }
    }
}
