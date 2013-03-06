package com.oboturov.ht.pig;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URL;
import java.util.HashMap;

/**
 * This class resolves shortened URLs by searching over the
 * <a href='code.google.com/p/shortenurl/wiki/URLShorteningServices'>code.google.com/p/shortenurl/wiki/URLShorteningServices</a>
 * list.
 * @author aoboturov
 */
public class InvalidUrlRemover extends EvalFunc<DataBag> {

    public static final HashMap<String, Boolean> DEAD_SHORTENERS = new HashMap<String, Boolean>(128);
    public static final HashMap<String, Boolean> VALID_SHORTENERS = new HashMap<String, Boolean>(128);

    private static final String DEAD_SERVICES_LIST = "/com/oboturov/ht/stage3/dead.txt";
    private static final String VALID_SERVICES_LIST = "/com/oboturov/ht/stage3/valid.txt";

    static {
        final InputStream deadShortenersInputStream = InvalidUrlRemover.class.getResourceAsStream(DEAD_SERVICES_LIST);
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
        final InputStream validShortenersInputStream = InvalidUrlRemover.class.getResourceAsStream(VALID_SERVICES_LIST);
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

    public static boolean isDeadShortenerUrl(final URL url) {
        return DEAD_SHORTENERS.containsKey(url.getHost());
    }

    public static boolean isValidShortenerUrl(final URL url) {
        return VALID_SHORTENERS.containsKey(url.getHost());
    }

    // Expect a tuple with an URL as an argument.
    @Override
    public DataBag exec(final Tuple input) throws IOException {
        try {
            if (input == null || input.size() == 0) return null;
            final DataBag urlTuples = (DataBag)input.get(0);
            final DataBag filteredUrls = new DefaultDataBag();
            for (final Tuple tuple : urlTuples) {
                String link = (String)tuple.get(0);
                if (!link.startsWith("http://") && !link.startsWith("https://")) {
                    link = "http://"+link;
                }
                final URL url = new URL(link);
                if (isDeadShortenerUrl(url)) continue;
                if (isValidShortenerUrl(url)) continue;
                final Tuple correctUrl = new DefaultTuple();
                correctUrl.append(link);
                filteredUrls.add(correctUrl);
            }
            return filteredUrls;
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }
}
