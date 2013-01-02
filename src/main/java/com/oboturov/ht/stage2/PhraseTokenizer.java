package com.oboturov.ht.stage2;

import com.oboturov.ht.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.util.Version.LUCENE_40;

/**
 * @author aoboturov
 */
public class PhraseTokenizer {

    public static class PhraseTokenizerMap extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(PhraseTokenizerMap.class);

        enum Counters {
            LANGUAGE_NOT_SUPPORTED, PRODUCED_STEMMED_ENTITIES
        }

        private static final Version usedLuceneVersion = LUCENE_40;

        private static final ThreadLocal<Map<String, Analyzer>> analyzerMap = new ThreadLocal<Map<String, Analyzer>>() {
            @Override
            protected Map<String, Analyzer> initialValue() {
                final Map<String, Analyzer> map = new HashMap<>();

                map.put("ar", new org.apache.lucene.analysis.ar.ArabicAnalyzer(usedLuceneVersion));
                map.put("bg", new org.apache.lucene.analysis.bg.BulgarianAnalyzer(usedLuceneVersion));
                map.put("cs", new org.apache.lucene.analysis.cz.CzechAnalyzer(usedLuceneVersion));
                map.put("da", new org.apache.lucene.analysis.da.DanishAnalyzer(usedLuceneVersion));
                map.put("de", new org.apache.lucene.analysis.de.GermanAnalyzer(usedLuceneVersion));
                map.put("el", new org.apache.lucene.analysis.el.GreekAnalyzer(usedLuceneVersion));
                map.put("en", new org.apache.lucene.analysis.en.EnglishAnalyzer(usedLuceneVersion));
                map.put("es", new org.apache.lucene.analysis.es.SpanishAnalyzer(usedLuceneVersion));
                map.put("fa", new org.apache.lucene.analysis.fa.PersianAnalyzer(usedLuceneVersion));
                map.put("fi", new org.apache.lucene.analysis.fi.FinnishAnalyzer(usedLuceneVersion));
                map.put("fr", new org.apache.lucene.analysis.fr.FrenchAnalyzer(usedLuceneVersion));
                map.put("hi", new org.apache.lucene.analysis.hi.HindiAnalyzer(usedLuceneVersion));
                map.put("hu", new org.apache.lucene.analysis.hu.HungarianAnalyzer(usedLuceneVersion));
                map.put("id", new org.apache.lucene.analysis.id.IndonesianAnalyzer(usedLuceneVersion));
                map.put("it", new org.apache.lucene.analysis.it.ItalianAnalyzer(usedLuceneVersion));
                map.put("nl", new org.apache.lucene.analysis.nl.DutchAnalyzer(usedLuceneVersion));
                map.put("no", new org.apache.lucene.analysis.no.NorwegianAnalyzer(usedLuceneVersion));
                map.put("pt", new org.apache.lucene.analysis.pt.PortugueseAnalyzer(usedLuceneVersion));
                map.put("ro", new org.apache.lucene.analysis.ro.RomanianAnalyzer(usedLuceneVersion));
                map.put("ru", new org.apache.lucene.analysis.ru.RussianAnalyzer(usedLuceneVersion));
                map.put("sv", new org.apache.lucene.analysis.sv.SwedishAnalyzer(usedLuceneVersion));
                map.put("th", new org.apache.lucene.analysis.th.ThaiAnalyzer(usedLuceneVersion));
                map.put("tr", new org.apache.lucene.analysis.tr.TurkishAnalyzer(usedLuceneVersion));
                map.put("uk", new org.apache.lucene.analysis.en.EnglishAnalyzer(usedLuceneVersion));
                map.put("ja", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
                map.put("ko", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
                map.put("zh-cn", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
                map.put("zh-tw", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));

                // CJKAnalyzer provider some erroneous results although was supposed to work.
                //
                // analyzerMap.put("ja", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
                // analyzerMap.put("ko", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
                // analyzerMap.put("zh-cn", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
                // analyzerMap.put("zh-tw", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));

                return map;
            }
        };

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (!KeyType.RAW_TEXT.equals(nuplet.getKeyword().getType())) {
                throw new RuntimeException("This Mapper is supposed to be used only with raw nuplets");
            }

            final Analyzer analyzer = analyzerMap.get().get(nuplet.getLang());
            if (analyzer == null) {
                // Skip those phrases which could not be analysed.
                reporter.incrCounter(Counters.LANGUAGE_NOT_SUPPORTED, 1l);
                return;
            }

            final TokenStream tokenStream = analyzer.tokenStream("a", new StringReader(nuplet.getKeyword().getValue()));
            tokenStream.addAttribute(CharTermAttribute.class);
            try {
                tokenStream.reset();
                while (tokenStream.incrementToken()) {
                    final CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
                    final String tokenValue = attribute.toString();
                    final Nuplet newNuplet = new Nuplet();
                    newNuplet.setUser(nuplet.getUser());
                    newNuplet.setLang(nuplet.getLang());
                    newNuplet.setKeyword(new Keyword(KeyType.STEMMED_ENTITY, tokenValue));
                    newNuplet.setItem(nuplet.getItem());
                    // Produce new nuplet with updated key value.
                    output.collect(NullWritable.get(), newNuplet);
                    reporter.incrCounter(Counters.PRODUCED_STEMMED_ENTITIES, 1l);
                }
                tokenStream.end();
            } catch (Exception ex) {
                logger.error("Token stream error", ex);
            } finally {
                tokenStream.close();
            }
        }
    }

}
