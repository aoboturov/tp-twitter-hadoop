package com.oboturov.ht.stage0;

import com.oboturov.ht.Tweet;
import com.oboturov.ht.pig.SanitizeTweetText;
import com.oboturov.ht.pig.SanitizeUserId;
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
            ILLEGAL_DATE, NON_NORMALIZABLE_USER_NAME, TWEETS_READ, EMPTY_POSTS
        }

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

        public void map(final LongWritable offset, final Text value, final OutputCollector<NullWritable, Tweet> output, final Reporter reporter) throws IOException {
            final String line = value.toString();
            // Reset previous tweets here.
            if (line == null || line.isEmpty()) {
                skipTweet = false;
                reset();
                return;
            }
            if (line.length() < 3) {
                skipTweet = true;
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
                        logger.error(String.format("At %d, Wrong date format for date: '%s'", offset.get(), text));
                        reporter.setStatus("Detected illegal date");
                        reporter.incrCounter(Counters.ILLEGAL_DATE, 1l);
                    }
                    return;
                case 'U':
                    this.user = SanitizeUserId.sanitize(text);
                    if (this.user == null) {
                        logger.error(String.format("At %d, Not normalized user name: '%s'", offset.get(), text));
                        skipTweet = true;
                        reporter.setStatus("Detected non-normalizable user name");
                        reporter.incrCounter(Counters.NON_NORMALIZABLE_USER_NAME, 1l);
                    }
                    return;
                case 'W':
                    this.post = SanitizeTweetText.sanitize(text);
                    if (this.post == null) {
                        skipTweet = true;
                        reporter.incrCounter(Counters.EMPTY_POSTS, 1l);
                    }
                    break;
                default:
                    skipTweet = true;
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
