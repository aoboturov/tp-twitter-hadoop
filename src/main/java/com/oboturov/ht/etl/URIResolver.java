package com.oboturov.ht.etl;

import com.oboturov.ht.Item;
import com.oboturov.ht.ItemType;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.User;
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
    public static class Map extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {

        public static final HashMap<String, Boolean> DEAD_SERVICES = new HashMap<String, Boolean>(128);
        public static final HashMap<String, Boolean> VALID_SHORTENERS = new HashMap<String, Boolean>(128);

        static {
            HttpURLConnection.setFollowRedirects(false);
            URLConnection.setDefaultAllowUserInteraction(false);
        }

        static {
            // To be decided
            // #1
            DEAD_SERVICES.put("ir.pe", true);
            // These services are dead or no longer working
            // #1
            DEAD_SERVICES.put("2su.de", true);
            // #2
            DEAD_SERVICES.put("2ze.us", true);
            // #3
            DEAD_SERVICES.put("3.ly", true);
            // #4
            DEAD_SERVICES.put("301.to", true);
            // #5
            DEAD_SERVICES.put("307.to", true);
            // #6
            DEAD_SERVICES.put("9mp.com", true);
            // #7
            DEAD_SERVICES.put("a.gd", true);
            // #8
            DEAD_SERVICES.put("a.nf", true);
            // #9
            DEAD_SERVICES.put("abbr.com", true);
            // #10
            DEAD_SERVICES.put("aurls.info", true);
            // #11
            DEAD_SERVICES.put("bloat.me", true);
            // #12
            DEAD_SERVICES.put("buk.me", true);
            // #13
            DEAD_SERVICES.put("clk.my", true);
            // #14
            DEAD_SERVICES.put("crum.pl", true);
            // #15 - is not really a shortener.
            // DEAD_SERVICES.put("digg.com/tools/diggbar", true);
            // #16
            DEAD_SERVICES.put("fly2.ws", true);
            // #17
            DEAD_SERVICES.put("foxyurl.com", true);
            // #18
            DEAD_SERVICES.put("gl.am", true);
            // #19
            DEAD_SERVICES.put("good.ly", true);
            // #20
            DEAD_SERVICES.put("gurl.es", true);
            // #21
            DEAD_SERVICES.put("hao.jp", true);
            // #22
            DEAD_SERVICES.put("hex.io", true);
            // #23
            DEAD_SERVICES.put("hop.im", true);
            // #24
            DEAD_SERVICES.put("hurl.ws", true);
            // #25
            DEAD_SERVICES.put("idek.net", true);
            // #26
            DEAD_SERVICES.put("j2j.de", true);
            // #27
            DEAD_SERVICES.put("k.vu", true);
            // #28
            DEAD_SERVICES.put("ketkp.in", true);
            // #29
            DEAD_SERVICES.put("kissa.be", true);
            // #30
            DEAD_SERVICES.put("kisa.ch", true);
            // #31
            DEAD_SERVICES.put("kl.am", true);
            // #32
            DEAD_SERVICES.put("kots.nu", true);
            // #33
            DEAD_SERVICES.put("ktzr.us", true);
            // #34
            DEAD_SERVICES.put("lin.cr", true);
            // #35
            DEAD_SERVICES.put("linxfix.de", true);
            // #36
            DEAD_SERVICES.put("tiny.pl", true);
        }

        static {
            // Services in alphabetic order:
            // #1
            VALID_SHORTENERS.put("2.gp", true);
            // #2
            VALID_SHORTENERS.put("2.ly", true);
            // #3
            VALID_SHORTENERS.put("arm.in", true);
            // #4
            VALID_SHORTENERS.put("bit.ly", true);
            // #5
            VALID_SHORTENERS.put("chilp.it", true);
            // #6
            VALID_SHORTENERS.put("cli.gs", true);
            // #7
            VALID_SHORTENERS.put("clop.in", true);
            // #8
            VALID_SHORTENERS.put("coge.la", true);
            // #9
            VALID_SHORTENERS.put("durl.me", true);
            // #10
            VALID_SHORTENERS.put("fon.gs", true);
            // #11
            VALID_SHORTENERS.put("fwd4.me", true);
            // #12
            VALID_SHORTENERS.put("gkurl.us", true);
            // #13
            VALID_SHORTENERS.put("goo.gl", true);
            // #14
            VALID_SHORTENERS.put("hj.to", true);
            // #15
            VALID_SHORTENERS.put("hurl.no", true);
            // #16
            VALID_SHORTENERS.put("ikr.me", true);
            // #17
            VALID_SHORTENERS.put("is.gd", true);
            // #18
            VALID_SHORTENERS.put("irt.me", true);
            // #19
            VALID_SHORTENERS.put("j.mp", true);
            // #20
            VALID_SHORTENERS.put("jdem.cz", true);
            // #21
            VALID_SHORTENERS.put("kore.us", true);
            // #22
            VALID_SHORTENERS.put("krz.ch", true);
            // #23
            VALID_SHORTENERS.put("l.pr", true);
            // #24
            VALID_SHORTENERS.put("lin.io", true);
            // #25
            VALID_SHORTENERS.put("linkee.com", true);
            // #26
            VALID_SHORTENERS.put("lnk.by", true);
            // #27
            VALID_SHORTENERS.put("lnk.ly", true);
            // #28
            VALID_SHORTENERS.put("ly.my", true);
            // #29
            VALID_SHORTENERS.put("moourl.com", true);
            // #30
            VALID_SHORTENERS.put("metamark.net", true);
            // Some which were crossed out:
            // #1
            VALID_SHORTENERS.put("lnk.sk", true);
            // #2
            VALID_SHORTENERS.put("lt.tl", true);
            // #3
            VALID_SHORTENERS.put("lurl.no", true);
            // #4
            VALID_SHORTENERS.put("mangk.us", true);
            // Continuation of the list:
            // #1
            VALID_SHORTENERS.put("migre.me", true);
            // #2
            VALID_SHORTENERS.put("micurl.com", true);
            // #3
            VALID_SHORTENERS.put("min2.me", true);
            // #4
            VALID_SHORTENERS.put("minilink.org", true);
            // #4-bis
            VALID_SHORTENERS.put("lnk.nu", true);
            // #5
            VALID_SHORTENERS.put("minurl.fr", true);
            // #6
            VALID_SHORTENERS.put("mysp.in", true);
            // #7
            VALID_SHORTENERS.put("myurl.in", true);
            // #8
            VALID_SHORTENERS.put("nbx.ch", true);
            // #9
            VALID_SHORTENERS.put("ndurl.com", true);
            // #10
            VALID_SHORTENERS.put("nm.ly", true);
            // #11
            VALID_SHORTENERS.put("omf.gd", true);
            // #12
            VALID_SHORTENERS.put("ow.ly", true);
            // #13
            VALID_SHORTENERS.put("pendek.in", true);
            // #14
            VALID_SHORTENERS.put("pic.gd", true);
            // #15
            VALID_SHORTENERS.put("piko.me", true);
            // #16
            VALID_SHORTENERS.put("piurl.com", true);
            // #17
            VALID_SHORTENERS.put("plo.cc", true);
            // #18
            VALID_SHORTENERS.put("pnt.me", true);
            // #19
            VALID_SHORTENERS.put("pt2.me", true);
            // #20
            VALID_SHORTENERS.put("puke.it", true);
            // #21
            VALID_SHORTENERS.put("qik.li", true);
            // #22
            VALID_SHORTENERS.put("qr.cx", true);
            // #23
            VALID_SHORTENERS.put("qurl.com", true);
            // #24
            VALID_SHORTENERS.put("qux.in", true);
            // #25
            VALID_SHORTENERS.put("r.im", true);
            // #26
            VALID_SHORTENERS.put("rde.me", true);
            // #27
            VALID_SHORTENERS.put("p.ly", true);
            // #28
            VALID_SHORTENERS.put("retwt.me", true);
            // #29
            VALID_SHORTENERS.put("qik.li", true);
            // #30
            VALID_SHORTENERS.put("redir.ec", true);
            // #31
            VALID_SHORTENERS.put("ri.ms", true);
            // #32
            VALID_SHORTENERS.put("rnk.me", true);
            // #33
            VALID_SHORTENERS.put("rt.nu", true);
            // #34
            VALID_SHORTENERS.put("rubyurl.com", true);
            // #35
            VALID_SHORTENERS.put("safe.mn", true);
            // #36
            VALID_SHORTENERS.put("sai.ly", true);
            // #37
            VALID_SHORTENERS.put("7rz.de", true);
            // #38
            VALID_SHORTENERS.put("sl.ly", true);
            // #39
            VALID_SHORTENERS.put("shadyurl.com", true);
            // #40
            VALID_SHORTENERS.put("slki.ru", true);
            // #41
            VALID_SHORTENERS.put("sfu.ca", true);
            // #42
            VALID_SHORTENERS.put("shorl.com", true);
            // #43
            VALID_SHORTENERS.put("short.ie", true);
            // #44
            VALID_SHORTENERS.put("short.to", true);
            // #45
            VALID_SHORTENERS.put("s4c.in", true);
            // #46
            VALID_SHORTENERS.put("shortn.me", true);
            // #47
            VALID_SHORTENERS.put("shrt.ws", true);
            // #48
            VALID_SHORTENERS.put("shrtn.com", true);
            // #49
            VALID_SHORTENERS.put("shw.me", true);
            // #50
            VALID_SHORTENERS.put("siteo.us", true);
            // #51
            VALID_SHORTENERS.put("smallr.net", true);
            // #52
            VALID_SHORTENERS.put("smfu.in", true);
            // #53
            VALID_SHORTENERS.put("smfu.in", true);
            // #54
            VALID_SHORTENERS.put("snipurl.com", true);
            // #55
            VALID_SHORTENERS.put("snkr.me", true);
            // #56
            VALID_SHORTENERS.put("song.ly", true);
            // #57
            VALID_SHORTENERS.put("srnk.net", true);
            // #58
            VALID_SHORTENERS.put("su.pr", true);
            // #59
            VALID_SHORTENERS.put("swu.me", true);
            // #60
            VALID_SHORTENERS.put("tighturl.com", true);
            // #61
            VALID_SHORTENERS.put("timesurl.at", true);
            // #62
            VALID_SHORTENERS.put("tini.us", true);
            // #63
            VALID_SHORTENERS.put("tiny.cc", true);
            // #64
            VALID_SHORTENERS.put("tinypl.us", true);
            // #65
            VALID_SHORTENERS.put("tinyurl.com", true);
            // #66
            VALID_SHORTENERS.put("tllg.net", true);
            // #67
            VALID_SHORTENERS.put("to.je", true);
            // #68
            VALID_SHORTENERS.put("to.ly", true);
            // #69
            VALID_SHORTENERS.put("to.vg", true);
            // #70
            VALID_SHORTENERS.put("tr.im", true);
            // #71
            VALID_SHORTENERS.put("tra.kz", true);
            // #72
            VALID_SHORTENERS.put("trumpink.lt", true);
            // #73
            VALID_SHORTENERS.put("tsort.us", true);
            // #74
            VALID_SHORTENERS.put("tweet.me", true);
            // #75
            VALID_SHORTENERS.put("tweetburner.com", true);
            // #76
            VALID_SHORTENERS.put("twip.us", true);
            // #77
            VALID_SHORTENERS.put("twirl.at", true);
            // #78
            VALID_SHORTENERS.put("twtr.us", true);
            // #78-bis
            VALID_SHORTENERS.put("tw6.us", true);
            // #79
            VALID_SHORTENERS.put("u.nu", true);
            // #80
            VALID_SHORTENERS.put("uiop.me", true);
            // #81
            VALID_SHORTENERS.put("ur.ly", true);
            // #82
            VALID_SHORTENERS.put("urizy.com", true);
            // #83
            VALID_SHORTENERS.put("unfaker.it", true);
            // #84
            VALID_SHORTENERS.put("urlcorta.es", true);
            // #85
            VALID_SHORTENERS.put("url.ag", true);
            // #86
            VALID_SHORTENERS.put("urli.nl", true);
            // #87
            VALID_SHORTENERS.put("urloo.com", true);
            // #88
            VALID_SHORTENERS.put("urloo.com", true);
            // #89
            VALID_SHORTENERS.put("urlborg.com", true);
            // #90
            VALID_SHORTENERS.put("urlg.info", true);
            // #91
            VALID_SHORTENERS.put("urlz.at", true);
            // #92
            VALID_SHORTENERS.put("ooqx.com", true);
            // #93
            VALID_SHORTENERS.put("u.mavrev.com", true);
            // #94
            VALID_SHORTENERS.put("urlu.ms", true);
            // #95
            VALID_SHORTENERS.put("urlzen.com", true);
            // #96
            VALID_SHORTENERS.put("vb.ly", true);
            // #97
            VALID_SHORTENERS.put("vi.ly", true);
            // #98
            VALID_SHORTENERS.put("virl.com", true);
            // #99
            VALID_SHORTENERS.put("vl.am", true);
            // #100
            VALID_SHORTENERS.put("voizle.com", true);
            // #101
            VALID_SHORTENERS.put("vtc.es", true);
            // #102
            VALID_SHORTENERS.put("w3t.org", true);
            // #103
            VALID_SHORTENERS.put("wa.la", true);
            // #104
            VALID_SHORTENERS.put("xiy.net", true);
            // #105
            VALID_SHORTENERS.put("xrt.me", true);
            // #106
            VALID_SHORTENERS.put("xr.com", true);
            // #107
            VALID_SHORTENERS.put("xrl.in", true);
            // #108
            VALID_SHORTENERS.put("x.vu", true);
            // #109
            VALID_SHORTENERS.put("xxsurl.de", true);
            // #110
            VALID_SHORTENERS.put("z.pe", true);
            // #111
            VALID_SHORTENERS.put("zapt.in", true);
            // #112
            VALID_SHORTENERS.put("zi.me", true);
            // #113
            VALID_SHORTENERS.put("zi.pe", true);
            // #114
            VALID_SHORTENERS.put("zip.li", true);
            // #115
            VALID_SHORTENERS.put("zipmyurl.com", true);
            // #116
            VALID_SHORTENERS.put("zz.gd", true);
        }

        /**
         * Resolve each {@link com.oboturov.ht.ItemType.URL } to real URI if exists.
         * @param user
         * @param nuplet
         */
        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> output, final Reporter reporter) throws IOException {
            if (ItemType.URL.equals(nuplet.getItem().getType())) {
                try {
                    final URL url = new URL(nuplet.getItem().getValue());
                    if (DEAD_SERVICES.containsKey(url.getHost())) {// Discard those shortened URLs which could not be resolved.
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
                                        } else {
                                            return;
                                        }
                                        break;
                                    default:
                                        return;
                                }
                            } else {
                                // Do not handle non-HTTP links.
                                return;
                            }
                        } catch (IOException e) {
                            logger.error("URL connection error", e);
                            return;
                        }
                    }
                } catch (MalformedURLException e) {
                    logger.error("Illegal URL", e);
                    return;
                }
            }
            output.collect(user, nuplet);
        }
    }

}
