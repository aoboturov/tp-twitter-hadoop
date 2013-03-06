package com.oboturov.ht.pig;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.ErrorCode;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.Version.LUCENE_40;

/**
 * @author aoboturov
 */
public class TextTokenizer extends EvalFunc<DataBag> {

    public static final String[] DETECTABLE_LANGUAGES = {
            "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "fa", "fi", "fr", "gu", "he", "hi", "hr",
            "hu", "id", "it", "ja", "kn", "ko", "mk", "ml", "mr", "ne", "nl", "no", "pa", "pl", "pt", "ro", "ru",
            "sk", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
    };

    static {
        try {
            DetectorFactory.loadProfile(TextTokenizer.class.getClassLoader(), "languages", DETECTABLE_LANGUAGES);
        } catch (LangDetectException ex) {
            // OK if during testing it loads duplicates.
            if (!ErrorCode.DuplicateLangError.equals(ex.getCode())) {
                throw new RuntimeException("Language detector initialization error");
            }
        }
    }

    /**
     *
     * @param text for which language detection would be performed.
     * @return language code if detection succeeded or {@code null} if language was not detected.
     */
    public static String detectLanguage(final String text) {
        try {
            final Detector languageIdentifier = DetectorFactory.create();
            languageIdentifier.append(text);
            final String detectedLanguage = languageIdentifier.detect();
            return detectedLanguage;
        } catch (LangDetectException ex) {
            return null;
        }
    }

    private static final Version usedLuceneVersion = LUCENE_40;

    private static final ThreadLocal<Map<String, Analyzer>> analyzerMap = new ThreadLocal<Map<String, Analyzer>>() {
        @Override
        protected Map<String, Analyzer> initialValue() {
            final Map<String, Analyzer> map = new HashMap<String, Analyzer>();

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

    /**
     * @param text to be tokenized.
     * @param language the text is supposed to be written in.
     * @return a list of stems for a text.
     */
    public static List<String> tokenizeText(final String text, final String language) {
        final Analyzer analyzer = analyzerMap.get().get(language);

        // When language was not identified.
        if (analyzer == null) {
            return Collections.emptyList();
        }

        // When language was detected.
        try {
            final TokenStream tokenStream = analyzer.tokenStream("a", new StringReader(text));
            try {
                tokenStream.addAttribute(CharTermAttribute.class);
                tokenStream.reset();
                final List<String> tokens = Lists.newLinkedList();
                while (tokenStream.incrementToken()) {
                    final CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
                    final String tokenValue = attribute.toString();
                    tokens.add(tokenValue);
                }
                tokenStream.end();
                return tokens;
            } catch (final IOException e) {
                // Do nothing.
            } finally {
                tokenStream.close();
            }
        } catch (final IOException e)  {
            // Do nothing.
        }
        return Collections.emptyList();
    }

    @Override
    public DataBag exec(final Tuple input) throws IOException {
        if (input == null || input.size() == 0) return null;
        final String text = (String)input.get(0);
        final DataBag tokensBag = new DefaultDataBag();
        final String detectedLanguage = detectLanguage(text);
        if (detectedLanguage == null) return tokensBag;
        final List<String> tokens = tokenizeText(text, detectedLanguage);
        for (final String token : tokens) {
            final Tuple tuple = new DefaultTuple();
            tuple.append(token);
            tokensBag.add(tuple);
        }
        return tokensBag;
    }
}
