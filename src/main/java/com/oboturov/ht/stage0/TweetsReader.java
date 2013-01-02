package com.oboturov.ht.stage0;

import com.oboturov.ht.Tweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @author aoboturov
 */
public class TweetsReader {

    private final static Logger logger = Logger.getLogger(TweetsReader.class);

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, Tweet> {

        enum Counters {
            ILLEGAL_DATE, NON_NORMALIZABLE_USER_NAME, TWEETS_READ
        }

        private static final String HTTP_TWITTER_COM = "http://twitter.com/";
        private static final String HTTP_WWW_TWITTER_COM = "http://www.twitter.com/";
        private static final String EMPTY_POST_INDICATION = "No Post Title";

        private static ThreadLocal<DateFormat> dateFormat = new ThreadLocal<DateFormat>() {
            @Override
            protected synchronized DateFormat initialValue() {
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }
        };

        private transient Long time;
        private transient String user;
        private transient String post;

        private transient boolean skipTweet = false;

        private void reset() {
            time = null;
            user = null;
            post = null;
        }

        public void map(final LongWritable key, final Text value, final OutputCollector<NullWritable, Tweet> output, final Reporter reporter) throws IOException {
            final String line = value.toString();
            // Reset previous tweets here.
            if (line == null || line.isEmpty() || line.length() < 3) {
                skipTweet = false;
                reset();
                return;
            }
            final String text = line.substring(2).trim(); // Skip {T,U,V}\tab sequence and consider rest as content.
            final char lineType = line.charAt(0); // One of {T,U,V} characters.
            switch (lineType) {
                case 'T':
                    try {
                        this.time = dateFormat.get().parse(text).getTime();
                    } catch (final ParseException e) {
                        skipTweet = true;
                        logger.error(String.format("At %d, Wrong date format for date: '%s'", key.get(), text));
                        reporter.setStatus("Detected illegal date");
                        reporter.incrCounter(Counters.ILLEGAL_DATE, 1l);
                    }
                    return;
                case 'U':
                    if (text.startsWith(HTTP_TWITTER_COM)) {
                        this.user = "@"+text.substring(HTTP_TWITTER_COM.length());
                    } else if (text.startsWith(HTTP_WWW_TWITTER_COM)) {
                        this.user = "@"+text.substring(HTTP_WWW_TWITTER_COM.length());
                    } else {
                        this.user = text;
                        logger.error(String.format("At %d, Not normalized user name: '%s'", key.get(), text));
                        reporter.setStatus("Detected non-normalizable user name");
                        reporter.incrCounter(Counters.NON_NORMALIZABLE_USER_NAME, 1l);
                    }
                    return;
                case 'W':
                    this.post = text;
                    if (EMPTY_POST_INDICATION.equals(text)) {
                        skipTweet = true;
                    }
                    break;
                default:
                    return;
            }
            if (this.time == null || this.user == null || this.post == null) {
                skipTweet = true;
            }
            try {
                if (skipTweet) {
                    return;
                }
                output.collect(NullWritable.get(), new Tweet(this.user, this.time, this.post));
                reporter.incrCounter(Counters.TWEETS_READ, 1l);
            } finally {
                reset();
            }
        }
    }
}
