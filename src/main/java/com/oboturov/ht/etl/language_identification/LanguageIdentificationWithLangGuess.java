package com.oboturov.ht.etl.language_identification;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.ErrorCode;
import com.cybozu.labs.langdetect.LangDetectException;
import com.oboturov.ht.KeyType;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.User;
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

    public static class LanguageIdentificationMap extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {

        private final static Logger logger = Logger.getLogger(LanguageIdentificationMap.class);

        public LanguageIdentificationMap() {
            try {
                DetectorFactory.loadProfile(getClass().getClassLoader(), "languages",
                        LanguageIdentificationWithLangGuess.LanguageIdentificationMap.DETECTABLE_LANGUAGES);
            } catch (LangDetectException ex) {
                // OK if during testing it loads duplicates.
                if (!ErrorCode.DuplicateLangError.equals(ex.getCode())) {
                    logger.error("Language detector initialization error", ex);
                    throw new RuntimeException();
                }
            }
        }

        public static final String[] DETECTABLE_LANGUAGES = {
                "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "fa", "fi", "fr", "gu", "he", "hi", "hr",
                "hu", "id", "it", "ja", "kn", "ko", "mk", "ml", "mr", "ne", "nl", "no", "pa", "pl", "pt", "ro", "ru",
                "sk", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
        };

        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            if (KeyType.PLAIN_TEXT.equals(nuplet.getKeyword().getType())) {
                try {
                    final Detector languageIdentifier = DetectorFactory.create();
                    languageIdentifier.append(nuplet.getKeyword().getValue());
                    final String detectedLanguage = languageIdentifier.detect();
                    nuplet.setLang(detectedLanguage);
                } catch (LangDetectException ex) {
                    logger.error(ex);
                }
            }
            outputCollector.collect(user, nuplet);
        }
    }

    public static class FilterOutNonEnglishText extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {

        private static final Logger logger = Logger.getLogger(FilterOutNonEnglishText.class);

        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            if (KeyType.PLAIN_TEXT.equals(nuplet.getKeyword().getType())) {
                try {
                    final Detector languageIdentifier = DetectorFactory.create();
                    languageIdentifier.append(nuplet.getKeyword().getValue());
                    final String detectedLanguage = languageIdentifier.detect();
                    if ("en".equals(detectedLanguage) || "uk".equals(detectedLanguage)) {
                        nuplet.setLang("en");
                        outputCollector.collect(user, nuplet);
                    }
                } catch (LangDetectException ex) {
                    logger.error(ex);
                }
            }
        }
    }

}
