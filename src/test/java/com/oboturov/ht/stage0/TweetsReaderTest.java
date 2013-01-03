package com.oboturov.ht.stage0;

import com.oboturov.ht.Tweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.testng.annotations.Test;

import java.io.FileReader;
import java.io.LineNumberReader;

import static org.mockito.Mockito.*;
import static com.oboturov.ht.stage0.TweetsReader.Map.Counters.*;

/**
 * @author aoboturov
 */
public class TweetsReaderTest {

    @Test public void should_read_valid_tweet_data_test() throws Exception {
        final LineNumberReader lineReader = new LineNumberReader(new FileReader("src/test/data/com/oboturov/ht/stage0/valid-tweets-sample-file.txt"));
        final OutputCollector<NullWritable, Tweet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final TweetsReader.Map mapper = new TweetsReader.Map();

        String nextLine = lineReader.readLine();
        while (nextLine != null) {
            mapper.map(null, new Text(nextLine), output, reporter);
            nextLine = lineReader.readLine();
        }

        final long tOne = 1244333261000L,
                   tTwo = 1244333262000L;
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@hokiepokie728",
                        tOne,
                        "@fabro84 'Before the Storm' is a new song from the Jonas Brothers that is going to be on their new album. miley has a duet with nick on it!"
                ))
        );
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@annieng",
                        tTwo,
                        "is in LA now"
                ))
        );
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@caitlinhllywd",
                        tTwo,
                        "cleaning my room |:"
                ))
        );
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@deaconsnacks",
                        tTwo,
                        "@flytographer Cheer up Liz:)"
                ))
        );
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@gadgetsguru",
                        tTwo,
                        "Dinner!!! in Aurora, CO http://loopt.us/OvJBLw.t"
                ))
        );
        verify(output, atLeastOnce()).collect(
                isA(NullWritable.class),
                eq(new Tweet(
                        "@holland_hotels",
                        tTwo,
                        "Eden Amsterdam American Hotel (****) on various dates for 110 .. http://bit.ly/mbGoR"
                ))
        );

        verify(reporter, times(6)).incrCounter(eq(TWEETS_READ), anyLong());
    }

    @Test public void rejects_tweets_with_no_post_data_test() throws Exception {
        final LineNumberReader lineReader = new LineNumberReader(new FileReader("src/test/data/com/oboturov/ht/stage0/no-message-tweets-sample-file.txt"));
        final OutputCollector<NullWritable, Tweet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final TweetsReader.Map mapper = new TweetsReader.Map();

        String nextLine = lineReader.readLine();
        while (nextLine != null) {
            mapper.map(null, new Text(nextLine), output, reporter);
            nextLine = lineReader.readLine();
        }

        verify(output, times(2)).collect(
                isA(NullWritable.class),
                any(Tweet.class)
        );

        verify(reporter, times(2)).incrCounter(eq(EMPTY_POSTS), anyLong());
    }

    @Test public void rejects_tweets_with_non_normalizable_user_names_text() throws Exception {
        final LineNumberReader lineReader = new LineNumberReader(new FileReader("src/test/data/com/oboturov/ht/stage0/non-normalizable-user-name-sample-file.txt"));
        final OutputCollector<NullWritable, Tweet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final TweetsReader.Map mapper = new TweetsReader.Map();

        final LongWritable offset = new LongWritable(10l);
        String nextLine = lineReader.readLine();
        while (nextLine != null) {
            mapper.map(offset, new Text(nextLine), output, reporter);
            nextLine = lineReader.readLine();
        }

        verify(output, times(2)).collect(
                isA(NullWritable.class),
                any(Tweet.class)
        );

        verify(reporter, times(2)).incrCounter(eq(NON_NORMALIZABLE_USER_NAME), anyLong());
    }
}
