package com.oboturov.ht.stage2;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.ErrorCode;
import com.cybozu.labs.langdetect.LangDetectException;
import com.oboturov.ht.KeyType;
import com.oboturov.ht.Keyword;
import com.oboturov.ht.Nuplet;
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
            LANGUAGE_DETECTION_ERROR
        }

        public static final String[] DETECTABLE_LANGUAGES = {
                "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "fa", "fi", "fr", "gu", "he", "hi", "hr",
                "hu", "id", "it", "ja", "kn", "ko", "mk", "ml", "mr", "ne", "nl", "no", "pa", "pl", "pt", "ro", "ru",
                "sk", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
        };

        static {
            try {
                DetectorFactory.loadProfile(LanguageIdentificationMap.class.getClassLoader(), "languages",
                        LanguageIdentificationWithLangGuess.LanguageIdentificationMap.DETECTABLE_LANGUAGES);
            } catch (LangDetectException ex) {
                // OK if during testing it loads duplicates.
                if (!ErrorCode.DuplicateLangError.equals(ex.getCode())) {
                    logger.error("Language detector initialization error", ex);
                    throw new RuntimeException("Language detector initialization error");
                }
            }
        }

        @Override
        public void map(final NullWritable nothing, final Nuplet nuplet, final OutputCollector<NullWritable, Nuplet> output, final Reporter reporter) throws IOException {
            if (!KeyType.RAW_TEXT.equals(nuplet.getKeyword().getType())) {
                throw new RuntimeException("This Mapper is supposed to be used only with raw nuplets");
            }
            try {
                final Detector languageIdentifier = DetectorFactory.create();
                languageIdentifier.append(nuplet.getKeyword().getValue());
                final String detectedLanguage = languageIdentifier.detect();
                nuplet.setLang(detectedLanguage);
                reporter.incrCounter(Counters.class.getName(), detectedLanguage, 1l);
            } catch (LangDetectException ex) {
                nuplet.setKeyword(Keyword.NO_KEYWORD);
                reporter.incrCounter(Counters.LANGUAGE_DETECTION_ERROR, 1l);
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
