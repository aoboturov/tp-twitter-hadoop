package com.oboturov.ht.etl;

import com.oboturov.ht.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

/**
 * @author aoboturov
 */
public class Stanford_NLP_NER_ProcessorTest {

    @Test
    public void can_classify_text() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Stanford_NLP_NER_Processor.Map mapper = new Stanford_NLP_NER_Processor.Map(false);

        final User holland_hotels = new User();
        holland_hotels.setName("holland_hotels");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(holland_hotels);
        aNuplet.setItem(new Item(ItemType.AT, "flytographer"));
        aNuplet.setKeyword(new Keyword(KeyType.PLAIN_TEXT, "Twitter is in Los Angeles now Moscow William 15$ 14% White House Feb 31th"));

        mapper.map(
                holland_hotels,
                aNuplet,
                output,
                null
        );
    }

}
