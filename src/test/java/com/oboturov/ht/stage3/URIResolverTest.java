package com.oboturov.ht.stage3;

import com.oboturov.ht.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertFalse;

/**
 * @author aoboturov
 */
public class URIResolverTest {

    @Test public void must_load_dead_shorteners_list_test() {
        assertFalse(URIResolver.Map.DEAD_SHORTENERS.keySet().isEmpty(), "Dead shorteners list must not be empty");
    }

    @Test public void must_load_valid_shorteners_list_test() {
        assertFalse(URIResolver.Map.VALID_SHORTENERS.keySet().isEmpty(), "Valid shorteners list must not be empty");
    }

    @Test public void must_discard_malformed_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setItem(new Item(ItemType.URL, "http://:malformed.URL"));
        aNuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, "some text"));

        mapper.map(aUser, aNuplet, output, reporter);

        verify(output, never()).collect(any(User.class), any(Nuplet.class));
    }

    @Test public void must_normalize_protocol_to_http_if_missed_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Keyword aKeyword = new Keyword(KeyType.RAW_TEXT, "some text");

        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(aKeyword);
        aNuplet.setItem(new Item(ItemType.URL, "www.dandelionbubbles.com"));

        mapper.map(aUser, aNuplet, output, reporter);

        final Nuplet expectedNuplet = new Nuplet();
        expectedNuplet.setUser(aUser);
        expectedNuplet.setKeyword(aKeyword);
        expectedNuplet.setItem(new Item(ItemType.URL, "http://www.dandelionbubbles.com"));
        verify(output, atLeastOnce()).collect(any(User.class), eq(expectedNuplet));
        verifyNoMoreInteractions(output);
    }

    @Test public void must_discard_all_urls_shortened_by_downed_services_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, "some text"));

        for (final String host : URIResolver.Map.DEAD_SHORTENERS.keySet()) {
            final String shortenedUrl = String.format("http://%s/something.html", host);
            aNuplet.setItem(new Item(ItemType.URL, shortenedUrl));
            mapper.map(aUser, aNuplet, output, reporter);
        }

        // None of those URLs must be resolved.
        verifyZeroInteractions(output);
    }

    @Test public void does_not_discard_normal_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(new Keyword(KeyType.RAW_TEXT, "some text"));
        aNuplet.setItem(new Item(ItemType.URL, "https://google.com"));

        mapper.map(aUser, aNuplet, output, reporter);

        // None of those URLs must be resolved.
        verify(output, atLeastOnce()).collect(any(User.class), any(Nuplet.class));
        verifyNoMoreInteractions(output);
    }

    @Test public void must_unshorten_bitly_urls_test() throws IOException {
        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
        final Reporter reporter = mock(Reporter.class);
        final URIResolver.Map mapper = new URIResolver.Map();

        final User aUser = new User();
        aUser.setName("user");
        final Keyword aKeyword = new Keyword(KeyType.RAW_TEXT, "some text");

        final Nuplet aNuplet = new Nuplet();
        aNuplet.setUser(aUser);
        aNuplet.setKeyword(aKeyword);
        aNuplet.setItem(new Item(ItemType.URL, "http://bit.ly/mxkFBv"));
        mapper.map(aUser, aNuplet, output, reporter);

        final Nuplet bitlyTestNuplet = new Nuplet();
        bitlyTestNuplet.setUser(aUser);
        bitlyTestNuplet.setKeyword(aKeyword);
        bitlyTestNuplet.setItem(new Item(ItemType.URL, "https://bitly.com/"));
        verify(output, only()).collect(any(User.class), eq(bitlyTestNuplet));
        verifyNoMoreInteractions(output);
    }

// This is not a Unit-test per se : it DOES depend on network connectivity status.
//    @Test public void must_unshorten_snipurl_urls_test() throws IOException {
//        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);
//        final Reporter reporter = mock(Reporter.class);
//        final URIResolver.Map mapper = new URIResolver.Map();
//
//        final User aUser = new User();
//        aUser.setName("user");
//        final Keyword aKeyword = new Keyword(KeyType.RAW_TEXT, "some text");
//
//        final Nuplet aNuplet = new Nuplet();
//        aNuplet.setUser(aUser);
//        aNuplet.setKeyword(aKeyword);
//        aNuplet.setItem(new Item(ItemType.URL, "http://snipurl.com/25z8j8e"));
//        mapper.map(aUser, aNuplet, output, reporter);
//
//        final Nuplet snipurlTestNuplet = new Nuplet();
//        snipurlTestNuplet.setUser(aUser);
//        snipurlTestNuplet.setKeyword(aKeyword);
//        snipurlTestNuplet.setItem(new Item(ItemType.URL, "http://www.ej.ru/"));
//        verify(output, only()).collect(any(User.class), eq(snipurlTestNuplet));
//        verifyNoMoreInteractions(output);
//    }

}
