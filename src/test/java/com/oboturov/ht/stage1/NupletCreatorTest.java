package com.oboturov.ht.stage1;

import com.oboturov.ht.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static com.oboturov.ht.stage1.NupletCreator.Map.Counters.*;

/**
 * @author aoboturov
 */
public class NupletCreatorTest {

    @Test
    public void reads_valid_tweet_data() throws Exception {
        final OutputCollector<NullWritable, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final NupletCreator.Map mapper = new NupletCreator.Map();

        mapper.map(
                null,
                new Tweet("deaconsnacks", 100L, "@flytographer Cheer up Liz:)"),
                output,
                reporter
        );
        mapper.map(
                null,
                new Tweet("holland_hotels", 100L, "Eden Amsterdam American Hotel (****) on various dates for $110 .. http://bit.ly/mbGoR"),
                output,
                reporter
        );
        mapper.map(
                null,
                new Tweet("annieng", 100L, "is in LA now"),
                output,
                reporter
        );

        final User deaconsnacks = new User();
        deaconsnacks.setName("deaconsnacks");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(deaconsnacks);
        aNuplet.setItem(new Item(ItemType.AT, "flytographer"));
        aNuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, " Cheer up Liz:)"));

        verify(output, atLeastOnce()).collect(
                any(NullWritable.class),
                eq(aNuplet)
        );

        final User holland_hotels = new User();
        holland_hotels.setName("holland_hotels");
        final Nuplet bNuplet = new Nuplet();
        bNuplet.setUser(holland_hotels);
        bNuplet.setItem(new Item(ItemType.URL, "http://bit.ly/mbGoR"));
        bNuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, "Eden Amsterdam American Hotel (****) on various dates for $110 .. "));

        verify(output, atLeastOnce()).collect(
                any(NullWritable.class),
                eq(bNuplet)
        );

        // Do not process text only tweets.
        verify(output, atMost(2)).collect(any(NullWritable.class), any(Nuplet.class));

        verify(reporter, never()).incrCounter(eq(ILLEGAL_TWEET_ENTITY_TYPE), any(Long.class));
        verify(reporter, times(2)).incrCounter(eq(NUPLETS_GENERATED), any(Long.class));
    }

}
