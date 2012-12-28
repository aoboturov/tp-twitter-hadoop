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
            // #37-mine
            DEAD_SERVICES.put("tr.im", true);
            // #38-mine
            DEAD_SERVICES.put("rubyurl.com", true);
            // #39-mine
            DEAD_SERVICES.put("timesurl.at", true);
            // #40-mine
            DEAD_SERVICES.put("twip.us", true);
            // #41-mine
            DEAD_SERVICES.put("pic.gd", true);
            // #42-mine
            DEAD_SERVICES.put("arm.in", true);
            // #43-mine
            DEAD_SERVICES.put("clop.in", true);
            // #44-mine
            DEAD_SERVICES.put("coge.la", true);
            // #45-mine
            DEAD_SERVICES.put("fwd4.me", true);
            // #46-mine
            DEAD_SERVICES.put("hj.to", true);
            // #47-mine
            DEAD_SERVICES.put("hurl.no", true);
            // #48-mine
            DEAD_SERVICES.put("irt.me", true);
            // #49-mine
            DEAD_SERVICES.put("krz.ch", true);
            // #50-mine
            DEAD_SERVICES.put("l.pr", true);
            // #51-mine
            DEAD_SERVICES.put("lnk.ly", true);
            // #52-mine
            DEAD_SERVICES.put("ly.my", true);
            // #53-mine
            DEAD_SERVICES.put("mangk.us", true);
            // #54-mine
            DEAD_SERVICES.put("micurl.com", true);
            // #55-mine
            DEAD_SERVICES.put("min2.me", true);
            // #56-mine
            DEAD_SERVICES.put("ndurl.com", true);
            // #57-mine
            DEAD_SERVICES.put("nm.ly", true);
            // #58-mine
            DEAD_SERVICES.put("omf.gd", true);
            // #59-mine
            DEAD_SERVICES.put("piko.me", true);
            // #60-mine
            DEAD_SERVICES.put("piurl.com", true);
            DEAD_SERVICES.put("plo.cc", true);
            DEAD_SERVICES.put("pnt.me", true);
            DEAD_SERVICES.put("pt2.me", true);
            DEAD_SERVICES.put("puke.it", true);
            DEAD_SERVICES.put("qik.li", true);
            DEAD_SERVICES.put("qux.in", true);
            DEAD_SERVICES.put("r.im", true);
            DEAD_SERVICES.put("rde.me", true);
            DEAD_SERVICES.put("retwt.me", true);
            DEAD_SERVICES.put("qik.li", true);
            DEAD_SERVICES.put("rnk.me", true);
            DEAD_SERVICES.put("rt.nu", true);
            DEAD_SERVICES.put("sai.ly", true);
            DEAD_SERVICES.put("7rz.de", true);
            DEAD_SERVICES.put("sl.ly", true);
            DEAD_SERVICES.put("short.ie", true);
            DEAD_SERVICES.put("short.to", true);
            DEAD_SERVICES.put("s4c.in", true);
            DEAD_SERVICES.put("shrt.ws", true);
            DEAD_SERVICES.put("shrtn.com", true);
            DEAD_SERVICES.put("shw.me", true);
            DEAD_SERVICES.put("snipie.com", true);
            DEAD_SERVICES.put("snkr.me", true);
            DEAD_SERVICES.put("swu.me", true);
            DEAD_SERVICES.put("tini.us", true);
            DEAD_SERVICES.put("tinypl.us", true);
            DEAD_SERVICES.put("to.je", true);
            DEAD_SERVICES.put("trumpink.lt", true);
            DEAD_SERVICES.put("tsort.us", true);
            DEAD_SERVICES.put("tweet.me", true);
            DEAD_SERVICES.put("twtr.us", true);
            DEAD_SERVICES.put("tw6.us", true);
            DEAD_SERVICES.put("uiop.me", true);
            DEAD_SERVICES.put("urizy.com", true);
            DEAD_SERVICES.put("unfaker.it", true);
            DEAD_SERVICES.put("url.ag", true);
            DEAD_SERVICES.put("urli.nl", true);
            DEAD_SERVICES.put("urlborg.com", true);
            DEAD_SERVICES.put("urlg.info", true);
            DEAD_SERVICES.put("urlz.at", true);
            DEAD_SERVICES.put("ooqx.com", true);
            DEAD_SERVICES.put("u.mavrev.com", true);
            DEAD_SERVICES.put("urlu.ms", true);
            DEAD_SERVICES.put("urlzen.com", true);
            DEAD_SERVICES.put("vb.ly", true);
            DEAD_SERVICES.put("vi.ly", true);
            DEAD_SERVICES.put("virl.com", true);
            DEAD_SERVICES.put("voizle.com", true);
            DEAD_SERVICES.put("vtc.es", true);
            DEAD_SERVICES.put("xrt.me", true);
            DEAD_SERVICES.put("xr.com", true);
            DEAD_SERVICES.put("xrl.in", true);
            DEAD_SERVICES.put("z.pe", true);
            DEAD_SERVICES.put("zapt.in", true);
            DEAD_SERVICES.put("zi.me", true);
            DEAD_SERVICES.put("zi.pe", true);
            DEAD_SERVICES.put("zip.li", true);
            DEAD_SERVICES.put("zipmyurl.com", true);
            DEAD_SERVICES.put("zz.gd", true);
        }

        static {
            // Services in alphabetic order:
            // #1
            VALID_SHORTENERS.put("2.gp", true);
            // #2
            VALID_SHORTENERS.put("2.ly", true);
            // #3 - appears to be invalid.
            // VALID_SHORTENERS.put("arm.in", true);
            // #4
            VALID_SHORTENERS.put("bit.ly", true);
            // #5
            VALID_SHORTENERS.put("chilp.it", true);
            // #6
            VALID_SHORTENERS.put("cli.gs", true);
            // #7 - appears to be invalid.
            // VALID_SHORTENERS.put("clop.in", true);
            // #8 - appears to be invalid.
            // VALID_SHORTENERS.put("coge.la", true);
            // #9
            VALID_SHORTENERS.put("durl.me", true);
            // #10
            VALID_SHORTENERS.put("fon.gs", true);
            // #11 - appears to be invalid.
            // VALID_SHORTENERS.put("fwd4.me", true);
            // #12
            VALID_SHORTENERS.put("gkurl.us", true);
            // #13
            VALID_SHORTENERS.put("goo.gl", true);
            // #14 - appears to be invalid.
            // VALID_SHORTENERS.put("hj.to", true);
            // #15 - appears to be invalid.
            // VALID_SHORTENERS.put("hurl.no", true);
            // #16
            VALID_SHORTENERS.put("ikr.me", true);
            // #17
            VALID_SHORTENERS.put("is.gd", true);
            // #18 - appears to be invalid.
            // VALID_SHORTENERS.put("irt.me", true);
            // #19
            VALID_SHORTENERS.put("j.mp", true);
            // #20
            VALID_SHORTENERS.put("jdem.cz", true);
            // #21
            VALID_SHORTENERS.put("kore.us", true);
            // #22 - appears to be invalid.
            // VALID_SHORTENERS.put("krz.ch", true);
            // #23 - appears to be invalid.
            // VALID_SHORTENERS.put("l.pr", true);
            // #24
            VALID_SHORTENERS.put("lin.io", true);
            // #25
            VALID_SHORTENERS.put("linkee.com", true);
            // #26
            VALID_SHORTENERS.put("lnk.by", true);
            // #27 - appears to be invalid.
            // VALID_SHORTENERS.put("lnk.ly", true);
            // #28 - appears to be invalid.
            // VALID_SHORTENERS.put("ly.my", true);
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
            // #4 - appears to be invalid.
            // VALID_SHORTENERS.put("mangk.us", true);
            // Continuation of the list:
            // #1
            VALID_SHORTENERS.put("migre.me", true);
            // #2 - appears to be invalid.
            // VALID_SHORTENERS.put("micurl.com", true);
            // #3 - appears to be invalid.
            // VALID_SHORTENERS.put("min2.me", true);
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
            // #9 - appears to be invalid.
            // VALID_SHORTENERS.put("ndurl.com", true);
            // #10 - appears to be invalid.
            // VALID_SHORTENERS.put("nm.ly", true);
            // #11 - appears to be invalid.
            // VALID_SHORTENERS.put("omf.gd", true);
            // #12
            VALID_SHORTENERS.put("ow.ly", true);
            // #13
            VALID_SHORTENERS.put("pendek.in", true);
            // #14 - happens to be invalid.
            // VALID_SHORTENERS.put("pic.gd", true);
            // #15 - happens to be invalid.
            // VALID_SHORTENERS.put("piko.me", true);
            // #16 - happens to be invalid.
            // VALID_SHORTENERS.put("piurl.com", true);
            // #17 - happens to be invalid.
            // VALID_SHORTENERS.put("plo.cc", true);
            // #18 - appears to be invalid.
            // VALID_SHORTENERS.put("pnt.me", true);
            // #19 - appears to be invalid.
            // VALID_SHORTENERS.put("pt2.me", true);
            // #20 - appears to be invalid.
            // VALID_SHORTENERS.put("puke.it", true);
            // #21 - appears to be invalid.
            // VALID_SHORTENERS.put("qik.li", true);
            // #22
            VALID_SHORTENERS.put("qr.cx", true);
            // #23
            VALID_SHORTENERS.put("qurl.com", true);
            // #24 - appears to be invalid.
            // VALID_SHORTENERS.put("qux.in", true);
            // #25 - appears to be invalid.
            // VALID_SHORTENERS.put("r.im", true);
            // #26 - appears to be invalid.
            // VALID_SHORTENERS.put("rde.me", true);
            // #27 - Downed but redirect works.
            VALID_SHORTENERS.put("p.ly", true);
            // #28 - appears to be invalid.
            // VALID_SHORTENERS.put("retwt.me", true);
            // #29 - appears to be invalid.
            // VALID_SHORTENERS.put("qik.li", true);
            // #30
            VALID_SHORTENERS.put("redir.ec", true);
            // #31
            VALID_SHORTENERS.put("ri.ms", true);
            // #32 - appears to be invalid.
            // VALID_SHORTENERS.put("rnk.me", true);
            // #33 - appears to be invalid.
            // VALID_SHORTENERS.put("rt.nu", true);
            // #34 - downed
            // VALID_SHORTENERS.put("rubyurl.com", true);
            // #35
            VALID_SHORTENERS.put("safe.mn", true);
            // #36 - appears to be invalid.
            // VALID_SHORTENERS.put("sai.ly", true);
            // #37 - appears to be invalid.
            // VALID_SHORTENERS.put("7rz.de", true);
            // #38 - appears to be invalid.
            // VALID_SHORTENERS.put("sl.ly", true);
            // #39
            VALID_SHORTENERS.put("shadyurl.com", true);
            // #40
            VALID_SHORTENERS.put("slki.ru", true);
            // #41 - site of Simon Fraser University.
            // VALID_SHORTENERS.put("sfu.ca", true);
            // #42
            VALID_SHORTENERS.put("shorl.com", true);
            // #43 - appears to be invalid.
            // VALID_SHORTENERS.put("short.ie", true);
            // #44 - appears to be invalid.
            // VALID_SHORTENERS.put("short.to", true);
            // #45 - appears to be invalid.
            // VALID_SHORTENERS.put("s4c.in", true);
            // #46
            VALID_SHORTENERS.put("shortn.me", true);
            // #47 - appears to be invalid.
            // VALID_SHORTENERS.put("shrt.ws", true);
            // #48 - appears to be invalid.
            // VALID_SHORTENERS.put("shrtn.com", true);
            // #49 - appears to be invalid.
            // VALID_SHORTENERS.put("shw.me", true);
            // #50
            VALID_SHORTENERS.put("siteo.us", true);
            // #51 - Deactivated.
            VALID_SHORTENERS.put("smallr.net", true);
            // #52
            VALID_SHORTENERS.put("smfu.in", true);
            // #53 - appears to be invalid.
            // VALID_SHORTENERS.put("snipie.com", true);
            // #54
            VALID_SHORTENERS.put("snipurl.com", true);
            // #55 - appears to be invalid.
            // VALID_SHORTENERS.put("snkr.me", true);
            // #56
            VALID_SHORTENERS.put("song.ly", true);
            // #57
            VALID_SHORTENERS.put("srnk.net", true);
            // #58 - Redirects to meta-descriptor of Tiny-URLs.
            // VALID_SHORTENERS.put("su.pr", true);
            // #59 - appears to be invalid.
            // VALID_SHORTENERS.put("swu.me", true);
            // #60
            VALID_SHORTENERS.put("tighturl.com", true);
            // #61 - happens to be invalid.
            // VALID_SHORTENERS.put("timesurl.at", true);
            // #62 - happens to be invalid.
            // VALID_SHORTENERS.put("tini.us", true);
            // #63
            VALID_SHORTENERS.put("tiny.cc", true);
            // #64 - happens to be invalid.
            // VALID_SHORTENERS.put("tinypl.us", true);
            // #65
            VALID_SHORTENERS.put("tinyurl.com", true);
            // #66
            VALID_SHORTENERS.put("tllg.net", true);
            // #67 - happens to be invalid.
            // VALID_SHORTENERS.put("to.je", true);
            // #68
            VALID_SHORTENERS.put("to.ly", true);
            // #69
            VALID_SHORTENERS.put("to.vg", true);
            // #70 - happens to be invalid.
            // VALID_SHORTENERS.put("tr.im", true);
            // #71
            VALID_SHORTENERS.put("tra.kz", true);
            // #72 - happens to be invalid.
            // VALID_SHORTENERS.put("trumpink.lt", true);
            // #73 - happens to be invalid.
            // VALID_SHORTENERS.put("tsort.us", true);
            // #74 - happens to be invalid.
            // VALID_SHORTENERS.put("tweet.me", true);
            // #75
            VALID_SHORTENERS.put("tweetburner.com", true);
            // #76 - happens to be invalid.
            // VALID_SHORTENERS.put("twip.us", true);
            // #77 - suspended.
            VALID_SHORTENERS.put("twirl.at", true);
            // #78 - happens to be invalid.
            // VALID_SHORTENERS.put("twtr.us", true);
            // #78-bis - happens to be invalid.
            // VALID_SHORTENERS.put("tw6.us", true);
            // #79 - happens to be invalid.
            // VALID_SHORTENERS.put("u.nu", true);
            // #80 - happens to be invalid.
            // VALID_SHORTENERS.put("uiop.me", true);
            // #81
            VALID_SHORTENERS.put("ur.ly", true);
            // #82 - happens to be invalid.
            // VALID_SHORTENERS.put("urizy.com", true);
            // #83 - happens to be invalid.
            // VALID_SHORTENERS.put("unfaker.it", true);
            // #84
            VALID_SHORTENERS.put("urlcorta.es", true);
            // #85 - happens to be invalid.
            // VALID_SHORTENERS.put("url.ag", true);
            // #86
            VALID_SHORTENERS.put("url.ie", true);
            // #87 - happens to be invalid.
            // VALID_SHORTENERS.put("urli.nl", true);
            // #88
            VALID_SHORTENERS.put("urloo.com", true);
            // #89 - happens to be invalid.
            // VALID_SHORTENERS.put("urlborg.com", true);
            // #90 - happens to be invalid.
            // VALID_SHORTENERS.put("urlg.info", true);
            // #91 - happens to be invalid.
            // VALID_SHORTENERS.put("urlz.at", true);
            // #92 - happens to be invalid.
            // VALID_SHORTENERS.put("ooqx.com", true);
            // #93 - discontinued.
            // VALID_SHORTENERS.put("u.mavrev.com", true);
            // #94 - happens to be invalid.
            // VALID_SHORTENERS.put("urlu.ms", true);
            // #95 - happens to be invalid.
            // VALID_SHORTENERS.put("urlzen.com", true);
            // #96 - happens to be invalid.
            // VALID_SHORTENERS.put("vb.ly", true);
            // #97 - happens to be invalid.
            // VALID_SHORTENERS.put("vi.ly", true);
            // #98 - happens to be invalid.
            // VALID_SHORTENERS.put("virl.com", true);
            // #99
            VALID_SHORTENERS.put("vl.am", true);
            // #100 - happens to be invalid.
            // VALID_SHORTENERS.put("voizle.com", true);
            // #101 - happens to be invalid.
            // VALID_SHORTENERS.put("vtc.es", true);
            // #102 - happens to be invalid.
            // VALID_SHORTENERS.put("w3t.org", true);
            // #103 - happens to be invalid.
            // VALID_SHORTENERS.put("wa.la", true);
            // #104
            VALID_SHORTENERS.put("xiy.net", true);
            // #105 - happens to be invalid.
            // VALID_SHORTENERS.put("xrt.me", true);
            // #106 - happens to be invalid.
            // VALID_SHORTENERS.put("xr.com", true);
            // #107 - happens to be invalid.
            // VALID_SHORTENERS.put("xrl.in", true);
            // #108
            VALID_SHORTENERS.put("x.vu", true);
            // #109
            VALID_SHORTENERS.put("xxsurl.de", true);
            // #110 - happens to be invalid.
            // VALID_SHORTENERS.put("z.pe", true);
            // #111 - happens to be invalid.
            // VALID_SHORTENERS.put("zapt.in", true);
            // #112 - happens to be invalid.
            // VALID_SHORTENERS.put("zi.me", true);
            // #113 - happens to be invalid.
            // VALID_SHORTENERS.put("zi.pe", true);
            // #114 - happens to be invalid.
            // VALID_SHORTENERS.put("zip.li", true);
            // #115 - happens to be invalid.
            // VALID_SHORTENERS.put("zipmyurl.com", true);
            // #116 - happens to be invalid.
            // VALID_SHORTENERS.put("zz.gd", true);
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
                    String link = nuplet.getItem().getValue();
                    if (!link.startsWith("http://") && !link.startsWith("https://")) {
                        link = "http://"+link;
                        // Update URL value.
                        nuplet.setItem(new Item(ItemType.URL, link));
                    }
                    final URL url = new URL(link);
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
