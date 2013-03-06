package com.oboturov.ht.stage2;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.oboturov.ht.ItemType;
import com.oboturov.ht.KeyType;
import com.oboturov.ht.Keyword;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.pig.TextTokenizer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class LanguageIdentificationWithLangGuess {

    public static class LanguageIdentificationMap extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        private final static Logger logger = Logger.getLogger(LanguageIdentificationMap.class);

        enum Counters {
            NUPLETS_WITH_ITEMS_BUT_LANGUAGE_NOT_IDENTIFIED, NUPLETS_DISCARDED_BECAUSE_LANGUAGE_WAS_NOT_IDENTIFIED_AND_NO_ITEMS
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (!KeyType.RAW_TEXT.equals(nuplet.getKeyword().getType())) {
                throw new RuntimeException("This Mapper is supposed to be used only with raw nuplets");
            }
            final String detectedLanguage = TextTokenizer.detectLanguage(nuplet.getKeyword().getValue());
            if (detectedLanguage != null) {
                nuplet.setLang(detectedLanguage);
                reporter.incrCounter(Counters.class.getName(), detectedLanguage, 1l);
            } else {
                if (ItemType.NULL.equals(nuplet.getItem().getType())) {
                    reporter.incrCounter(Counters.NUPLETS_DISCARDED_BECAUSE_LANGUAGE_WAS_NOT_IDENTIFIED_AND_NO_ITEMS, 1l);
                    return;
                } else {
                    nuplet.setKeyword(Keyword.NO_KEYWORD);
                    reporter.incrCounter(Counters.NUPLETS_WITH_ITEMS_BUT_LANGUAGE_NOT_IDENTIFIED, 1l);
                }
            }
            output.collect(NullWritable.get(), nuplet);
        }
    }

    public static class FilterOutNonEnglishText extends MapReduceBase implements Mapper<NullWritable, Nuplet, NullWritable, Nuplet> {

        private static final Logger logger = Logger.getLogger(FilterOutNonEnglishText.class);

        enum Counters {
            REJECTED_AS_NON_ENGLISH, DETECTED_AS_ENGLISH, LANGUAGE_DETECTION_ERROR
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (KeyType.RAW_TEXT.equals(nuplet.getKeyword().getType())) {
                try {
                    final Detector languageIdentifier = DetectorFactory.create();
                    languageIdentifier.append(nuplet.getKeyword().getValue());
                    final String detectedLanguage = languageIdentifier.detect();
                    if ("en".equals(detectedLanguage) || "uk".equals(detectedLanguage)) {
                        nuplet.setLang("en");
                        output.collect(NullWritable.get(), nuplet);
                        reporter.incrCounter(Counters.DETECTED_AS_ENGLISH, 1l);
                    } else {
                        reporter.incrCounter(Counters.REJECTED_AS_NON_ENGLISH, 1l);
                    }
                } catch (LangDetectException ex) {
                    reporter.incrCounter(Counters.LANGUAGE_DETECTION_ERROR, 1l);
                }
            }
        }
    }

}
