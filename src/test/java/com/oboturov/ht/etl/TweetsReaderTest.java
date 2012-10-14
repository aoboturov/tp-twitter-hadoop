package com.oboturov.ht.etl;

import com.oboturov.ht.Tweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.testng.annotations.Test;

import java.io.FileReader;
import java.io.LineNumberReader;

import static org.mockito.Mockito.*;

/**
 * @author aoboturov
 */
public class TweetsReaderTest {

    @Test
    public void reads_valid_tweet_data() throws Exception {
        final LineNumberReader lineReader = new LineNumberReader(new FileReader("src/test/data/com/oboturov/ht/etl/valid-tweets-sample-file.txt"));
        final OutputCollector<LongWritable, Tweet> output = mock(OutputCollector.class);
        final TweetsReader.Map mapper = new TweetsReader.Map();

        String nextLine = lineReader.readLine();
        while (nextLine != null) {
            mapper.map(null, new Text(nextLine), output, null);
            nextLine = lineReader.readLine();
        }

        final long tOne = 1244333261000L,
                   tTwo = 1244333262000L;
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tOne)),
                eq(new Tweet(
                        "@hokiepokie728",
                        tOne,
                        "@fabro84 'Before the Storm' is a new song from the Jonas Brothers that is going to be on their new album. miley has a duet with nick on it!"
                ))
        );
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tTwo)),
                eq(new Tweet(
                        "@annieng",
                        tTwo,
                        "is in LA now"
                ))
        );
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tTwo)),
                eq(new Tweet(
                        "@caitlinhllywd",
                        tTwo,
                        "cleaning my room |:"
                ))
        );
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tTwo)),
                eq(new Tweet(
                        "@deaconsnacks",
                        tTwo,
                        "@flytographer Cheer up Liz:)"
                ))
        );
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tTwo)),
                eq(new Tweet(
                        "@gadgetsguru",
                        tTwo,
                        "Dinner!!! in Aurora, CO http://loopt.us/OvJBLw.t"
                ))
        );
        verify(output, atLeastOnce()).collect(
                eq(new LongWritable(tTwo)),
                eq(new Tweet(
                        "@holland_hotels",
                        tTwo,
                        "Eden Amsterdam American Hotel (****) on various dates for €110 .. http://bit.ly/mbGoR"
                ))
        );
    }

}
