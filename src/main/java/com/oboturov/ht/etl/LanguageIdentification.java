package com.oboturov.ht.etl;

import com.oboturov.ht.KeyType;
import com.oboturov.ht.Nuplet;
import com.oboturov.ht.User;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.apache.tika.language.LanguageIdentifier;

import java.io.IOException;

/**
 * @author aoboturov
 */
public class LanguageIdentification {

    public static class LanguageIdentificationMap extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {
        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            if (KeyType.PLAIN_TEXT.equals(nuplet.getKey().getType())) {
                final LanguageIdentifier languageIdentifier = new LanguageIdentifier(nuplet.getKey().getValue());
                if (languageIdentifier.isReasonablyCertain()) {
                    nuplet.setLang(languageIdentifier.getLanguage());
                }
            }
            outputCollector.collect(user, nuplet);
        }
    }

    public static class FilterOutNonEnglishText extends MapReduceBase implements Mapper<User, Nuplet, User, Nuplet> {

        private static final Logger logger = Logger.getLogger(FilterOutNonEnglishText.class);

        @Override
        public void map(final User user, final Nuplet nuplet, final OutputCollector<User, Nuplet> outputCollector, final Reporter reporter) throws IOException {
            if (KeyType.PLAIN_TEXT.equals(nuplet.getKey().getType())) {
                final LanguageIdentifier languageIdentifier = new LanguageIdentifier(nuplet.getKey().getValue());
                final String detectedLanguage = languageIdentifier.getLanguage();
                if (languageIdentifier.isReasonablyCertain() &&
                        ("en".equals(detectedLanguage) || "uk".equals(detectedLanguage)) ) {
                    nuplet.setLang("en");
                    outputCollector.collect(user, nuplet);
                }
            }
        }
    }

}
