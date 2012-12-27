package com.oboturov.ht.etl;

import com.oboturov.ht.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author aoboturov
 */
public class URIResolverTest {

    @Test public void must_discard_malformed_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setItem(new Item(ItemType.URL, "malformed URL"));
        aNuplet.setKeyword(new Keyword(KeyType.PLAIN_TEXT, "some text"));

        mapper.map(aUser, aNuplet, output, null);

        verify(output, never()).collect(any(User.class), any(Nuplet.class));
    }

    @Test public void must_discard_all_urls_shortened_by_downed_services_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(new Keyword(KeyType.PLAIN_TEXT, "some text"));

        for (final String host : URIResolver.Map.DEAD_SERVICES.keySet()) {
            final String shortenedUrl = String.format("http://%s/something.html", host);
            aNuplet.setItem(new Item(ItemType.URL, shortenedUrl));
            mapper.map(aUser, aNuplet, output, null);
        }

        // None of those URLs must be resolved.
        verifyZeroInteractions(output);
    }

    @Test public void do_not_discard_normal_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(new Keyword(KeyType.PLAIN_TEXT, "some text"));
        aNuplet.setItem(new Item(ItemType.URL, "https://google.com"));

        mapper.map(aUser, aNuplet, output, null);

        // None of those URLs must be resolved.
        verify(output, atLeastOnce()).collect(any(User.class), any(Nuplet.class));
        verifyNoMoreInteractions(output);
    }

    @Test public void must_unshorten_bitly_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Keyword aKeyword = new Keyword(KeyType.PLAIN_TEXT, "some text");

        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(aKeyword);
        aNuplet.setItem(new Item(ItemType.URL, "http://bit.ly/mxkFBv"));
        mapper.map(aUser, aNuplet, output, null);

        final Nuplet bitlyTestNuplet = new Nuplet();
        bitlyTestNuplet.setUser(aUser);
        bitlyTestNuplet.setKeyword(aKeyword);
        bitlyTestNuplet.setItem(new Item(ItemType.URL, "https://bitly.com/"));
        verify(output, only()).collect(any(User.class), eq(bitlyTestNuplet));
        verifyNoMoreInteractions(output);
    }

    @Test public void must_unshorten_snipurl_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Keyword aKeyword = new Keyword(KeyType.PLAIN_TEXT, "some text");

        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(aKeyword);
        aNuplet.setItem(new Item(ItemType.URL, "http://snipurl.com/25z8j8e"));
        mapper.map(aUser, aNuplet, output, null);

        final Nuplet snipurlTestNuplet = new Nuplet();
        snipurlTestNuplet.setUser(aUser);
        snipurlTestNuplet.setKeyword(aKeyword);
        snipurlTestNuplet.setItem(new Item(ItemType.URL, "http://www.ej.ru/"));
        verify(output, only()).collect(any(User.class), eq(snipurlTestNuplet));
        verifyNoMoreInteractions(output);
    }

}
