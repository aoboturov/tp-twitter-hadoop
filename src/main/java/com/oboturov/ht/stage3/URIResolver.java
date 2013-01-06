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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

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

        public static final HashMap<String, Boolean> DEAD_SHORTENERS = new HashMap<>(128);
        public static final HashMap<String, Boolean> VALID_SHORTENERS = new HashMap<>(128);

        static {
            HttpURLConnection.setFollowRedirects(false);
            URLConnection.setDefaultAllowUserInteraction(false);
        }

        private static final String DEAD_SERVICES_LIST = "/com/oboturov/ht/stage3/dead.txt";
        private static final String VALID_SERVICES_LIST = "/com/oboturov/ht/stage3/valid.txt";

        static {
            final InputStream deadShortenersInputStream = Map.class.getResourceAsStream(DEAD_SERVICES_LIST);
            final LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(deadShortenersInputStream));
            String serviceName;
            try {
                while ((serviceName = lineNumberReader.readLine()) != null) {
                    if (!serviceName.isEmpty()) {
                        DEAD_SHORTENERS.put(serviceName.trim(), true);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        static {
            final InputStream validShortenersInputStream = Map.class.getResourceAsStream(VALID_SERVICES_LIST);
            final LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(validShortenersInputStream));
            String serviceName;
            try {
                while ((serviceName = lineNumberReader.readLine()) != null) {
                    if (!serviceName.isEmpty()) {
                        VALID_SHORTENERS.put(serviceName.trim(), true);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        enum Counters {
            LINKS_TO_DEAD_SHORTENERS, SHORTENED_LINKS_RESOLVED, RESOLUTIONS_FAILED, ILLEGAL_URIS, TOTAL_URIS_ACCEPTED,
            NETWORK_CONNECTION_ERRORS
        }

        /**
         * Resolve each {@link com.oboturov.ht.ItemType#URL } to real URI if exists.
         * @param nuplet
         */
        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (ItemType.URL.equals(nuplet.getItem().getType())) {
                try {
                    String link = nuplet.getItem().getValue();
                    if (!link.startsWith("http://") && !link.startsWith("https://")) {
                        link = "http://"+link;
                        // Update URL value.
                        nuplet.setItem(new Item(ItemType.URL, link));
                    }
                    final URL url = new URL(link);
                    if (DEAD_SHORTENERS.containsKey(url.getHost())) {// Discard those shortened URLs which could not be resolved.
                        reporter.incrCounter(Counters.LINKS_TO_DEAD_SHORTENERS, 1l);
                        return;
                    }
                    if (VALID_SHORTENERS.containsKey(url.getHost())) {// Handle redirect to full URL.
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
