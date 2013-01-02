package com.oboturov.ht.stage2;

import com.oboturov.ht.Nuplet;
import com.oboturov.ht.ObjectMapperInstance;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class NupletsReader {
    private final static Logger logger = Logger.getLogger(NupletsReader.class);

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, Nuplet> {

        enum Counters {
            NUPLET_DESERIALIZATION_ERROR, NUPLETS_DESERIALIZED
        }

        @Override
        public void map(final LongWritable offset, final Text value, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            try {
                final String nupletSerializedToJson = value.toString();
                final Nuplet nuplet = ObjectMapperInstance.get().readValue(nupletSerializedToJson, Nuplet.class);
                output.collect(NullWritable.get(), nuplet);
                reporter.incrCounter(Counters.NUPLETS_DESERIALIZED, 1l);
            } catch (IOException e) {
                logger.error("Unable to read a generated Tweet", e);
                reporter.incrCounter(Counters.NUPLET_DESERIALIZATION_ERROR, 1l);
            }
        }
    }
}
