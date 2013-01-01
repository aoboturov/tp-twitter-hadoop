package com.oboturov.ht;

import org.apache.hadoop.mapred.JobConf;

/**
 * @author aoboturov
 */
public final class ConfigUtils {

    public static JobConf makeMapOutputCompressedWithBZip2(final JobConf conf) {
        conf.setBoolean(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.COMPRESS, true);
        conf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.BZip2Codec");
        return conf;
    }
}
