package com.oboturov.ht.etl;

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

    public static class PhraseTokenizerMap extends MapReduceBase implements Mapper<User, Nuplet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(PhraseTokenizerMap.class);

        private static final Version usedLuceneVersion = LUCENE_40;
        private static final Map<String, Analyzer> analyzerMap = new HashMap<String, Analyzer>();
        static {
            analyzerMap.put("ar", new org.apache.lucene.analysis.ar.ArabicAnalyzer(usedLuceneVersion));
            analyzerMap.put("bg", new org.apache.lucene.analysis.bg.BulgarianAnalyzer(usedLuceneVersion));
            analyzerMap.put("cs", new org.apache.lucene.analysis.cz.CzechAnalyzer(usedLuceneVersion));
            analyzerMap.put("da", new org.apache.lucene.analysis.da.DanishAnalyzer(usedLuceneVersion));
            analyzerMap.put("de", new org.apache.lucene.analysis.de.GermanAnalyzer(usedLuceneVersion));
            analyzerMap.put("el", new org.apache.lucene.analysis.el.GreekAnalyzer(usedLuceneVersion));
            analyzerMap.put("en", new org.apache.lucene.analysis.en.EnglishAnalyzer(usedLuceneVersion));
            analyzerMap.put("es", new org.apache.lucene.analysis.es.SpanishAnalyzer(usedLuceneVersion));
            analyzerMap.put("fa", new org.apache.lucene.analysis.fa.PersianAnalyzer(usedLuceneVersion));
            analyzerMap.put("fi", new org.apache.lucene.analysis.fi.FinnishAnalyzer(usedLuceneVersion));
            analyzerMap.put("fr", new org.apache.lucene.analysis.fr.FrenchAnalyzer(usedLuceneVersion));
            analyzerMap.put("hi", new org.apache.lucene.analysis.hi.HindiAnalyzer(usedLuceneVersion));
            analyzerMap.put("hu", new org.apache.lucene.analysis.hu.HungarianAnalyzer(usedLuceneVersion));
            analyzerMap.put("id", new org.apache.lucene.analysis.id.IndonesianAnalyzer(usedLuceneVersion));
            analyzerMap.put("it", new org.apache.lucene.analysis.it.ItalianAnalyzer(usedLuceneVersion));
            analyzerMap.put("nl", new org.apache.lucene.analysis.nl.DutchAnalyzer(usedLuceneVersion));
            analyzerMap.put("no", new org.apache.lucene.analysis.no.NorwegianAnalyzer(usedLuceneVersion));
            analyzerMap.put("pt", new org.apache.lucene.analysis.pt.PortugueseAnalyzer(usedLuceneVersion));
            analyzerMap.put("ro", new org.apache.lucene.analysis.ro.RomanianAnalyzer(usedLuceneVersion));
            analyzerMap.put("ru", new org.apache.lucene.analysis.ru.RussianAnalyzer(usedLuceneVersion));
            analyzerMap.put("sv", new org.apache.lucene.analysis.sv.SwedishAnalyzer(usedLuceneVersion));
            analyzerMap.put("th", new org.apache.lucene.analysis.th.ThaiAnalyzer(usedLuceneVersion));
            analyzerMap.put("tr", new org.apache.lucene.analysis.tr.TurkishAnalyzer(usedLuceneVersion));
            analyzerMap.put("uk", new org.apache.lucene.analysis.en.EnglishAnalyzer(usedLuceneVersion));
            analyzerMap.put("ja", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
            analyzerMap.put("ko", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
            analyzerMap.put("zh-cn", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
            analyzerMap.put("zh-tw", new org.apache.lucene.analysis.standard.StandardAnalyzer(usedLuceneVersion));
// CJKAnalyzer provider some erroneous results although was supposed to work.
//            analyzerMap.put("ja", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
//            analyzerMap.put("ko", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
//            analyzerMap.put("zh-cn", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
//            analyzerMap.put("zh-tw", new org.apache.lucene.analysis.cjk.CJKAnalyzer(usedLuceneVersion));
        }

        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            final Analyzer analyzer = analyzerMap.get(nuplet.getLang());
            if (analyzer == null) {
                // Skip those phrases which could not be analysed.
                return;
            }
            if (!KeyType.PLAIN_TEXT.equals(nuplet.getKeyword().getType())) {
                // Analyse only plain text.
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
                    outputCollector.collect(NullWritable.get(), newNuplet);
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
