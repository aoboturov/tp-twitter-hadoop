package com.oboturov.ht.etl;

import com.oboturov.ht.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.testng.annotations.Test;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author aoboturov
 */
public class NupletCreatorTest {

    @Test
    public void reads_valid_tweet_data() throws Exception {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final NupletCreator.Map mapper = new NupletCreator.Map();

        mapper.map(
                null,
                new Tweet("deaconsnacks", 100L, "@flytographer Cheer up Liz:)"),
                output,
                null
        );
        mapper.map(
                null,
                new Tweet("holland_hotels", 100L, "Eden Amsterdam American Hotel (****) on various dates for €110 .. http://bit.ly/mbGoR"),
                output,
                null
        );
        mapper.map(
                null,
                new Tweet("annieng", 100L, "is in LA now"),
                output,
                null
        );

        final User deaconsnacks = new User();
        deaconsnacks.setName("deaconsnacks");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(deaconsnacks);
        aNuplet.setItem(new Item(ItemType.AT, "flytographer"));
        aNuplet.setKeys(" Cheer up Liz:)");

        verify(output, atLeastOnce()).collect(
                eq(deaconsnacks),
                eq(aNuplet)
        );

        final User holland_hotels = new User();
        holland_hotels.setName("holland_hotels");
        final Nuplet bNuplet = new Nuplet();
        bNuplet.setUser(holland_hotels);
        bNuplet.setItem(new Item(ItemType.URL, "http://bit.ly/mbGoR"));
        bNuplet.setKeys("Eden Amsterdam American Hotel (****) on various dates for €110 .. ");

        verify(output, atLeastOnce()).collect(
                eq(holland_hotels),
                eq(bNuplet)
        );

        // Do not process text only tweets.
        verify(output, atMost(2)).collect(any(User.class), any(Nuplet.class));
    }

}
