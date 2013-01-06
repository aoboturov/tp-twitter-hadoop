package com.oboturov.ht.stage3;

import com.oboturov.ht.Item;
import com.oboturov.ht.ItemType;
import com.oboturov.ht.Nuplet;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * @author aoboturov
 */
public class URIResolver {

    private final static Logger logger = Logger.getLogger(URIResolver.class);

    /**
     * This class resolves shortened URLs by searching over the
     * <a href='code.google.com/p/shortenurl/wiki/URLShorteningServices'>code.google.com/p/shortenurl/wiki/URLShorteningServices</a>
     * list.
     */
    public static class Map extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        static {
            HttpURLConnection.setFollowRedirects(false);
            URLConnection.setDefaultAllowUserInteraction(false);
        }

        enum Counters {
            SHORTENED_LINKS_RESOLVED, RESOLUTIONS_FAILED, ILLEGAL_URIS, TOTAL_URIS_ACCEPTED, NETWORK_CONNECTION_ERRORS
        }

        /**
         * Resolve each {@link com.oboturov.ht.ItemType#URL } to real URI if exists.
         */
        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (ItemType.URL.equals(nuplet.getItem().getType())) {
                try {
                    String link = nuplet.getItem().getValue();
                    final URL url = new URL(link);
                    try {
                        final URLConnection connection = url.openConnection();
                        connection.setUseCaches(true);
                        if ( connection instanceof HttpURLConnection) {
                            final HttpURLConnection httpURLConnection = (HttpURLConnection)connection;
                            httpURLConnection.setRequestMethod("HEAD");
                            httpURLConnection.connect();
                            httpURLConnection.getContent();
                            switch (httpURLConnection.getResponseCode()) {
                                // Redirect to 302, 303 should be handled by the connection.
                                case HttpURLConnection.HTTP_MOVED_PERM:
                                case HttpURLConnection.HTTP_MOVED_TEMP:
                                case HttpURLConnection.HTTP_SEE_OTHER:
                                    final String location = httpURLConnection.getHeaderField("Location");
                                    if (location != null) {
                                        final URL locationUrl = new URL(location);
                                        nuplet.setItem(new Item(ItemType.URL, location));
                                        reporter.incrCounter(Counters.SHORTENED_LINKS_RESOLVED, 1l);
                                    } else {
                                        reporter.incrCounter(Counters.RESOLUTIONS_FAILED, 1l);
                                        return;
                                    }
                                    break;
                                default:
                                    return;
                            }
                        } else {
                            // Do not handle non-HTTP links.
                            reporter.incrCounter(Counters.ILLEGAL_URIS, 1l);
                            return;
                        }
                    } catch (IOException e) {
                        logger.error("URL connection error", e);
                        reporter.incrCounter(Counters.NETWORK_CONNECTION_ERRORS, 1l);
                        return;
                    }
                } catch (MalformedURLException e) {
                    logger.error("Illegal URL", e);
                    reporter.incrCounter(Counters.ILLEGAL_URIS, 1l);
                    return;
                }
            }
            output.collect(nothing, nuplet);
            reporter.incrCounter(Counters.TOTAL_URIS_ACCEPTED, 1l);
        }
    }
}
