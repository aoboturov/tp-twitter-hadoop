package com.oboturov.ht.etl.language_identification;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.oboturov.ht.*;
import org.apache.hadoop.mapred.OutputCollector;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author aoboturov
 */
public class LanguageIdentificationWithLangGuessTest {

    @Test
    public void lang_guess_loads_all_required_language_profiles() throws LangDetectException {
        new LanguageIdentificationWithLangGuess.LanguageIdentificationMap();
    }

    @Test
    public void can_detect_simple_phrases() throws IOException, LangDetectException {
        final LanguageIdentificationWithLangGuess.LanguageIdentificationMap mapper =
                new LanguageIdentificationWithLangGuess.LanguageIdentificationMap();
        DetectorFactory.setSeed(0L);

        final OutputCollector<User, Nuplet> output = mock(OutputCollector.class);


        final User anUser = new User();
        anUser.setName("anuser");

        final Nuplet englishTextNuplet = new Nuplet();
        englishTextNuplet.setUser(anUser);
        englishTextNuplet.setItem(new Item(ItemType.AT, "otheruser"));
        englishTextNuplet.setKey(new Key(KeyType.PLAIN_TEXT, "Bus is pulling out now. We gotta be in LA by 8 to check into the Paragon."));
        mapper.map(
                anUser,
                englishTextNuplet,
                output,
                null
        );
        verify(output, atLeastOnce()).collect(eq(anUser), argThat(new IdentifiedLanguageMatcher("en")));

        final Nuplet japaneseTextNuplet = new Nuplet();
        japaneseTextNuplet.setUser(anUser);
        japaneseTextNuplet.setItem(new Item(ItemType.AT, "otheruser"));
        japaneseTextNuplet.setKey(new Key(KeyType.PLAIN_TEXT, "灰を灰皿に落とそうとすると高確率でヘッドセットの線を根性焼きする形になるんだが"));
        mapper.map(
                anUser,
                japaneseTextNuplet,
                output,
                null
        );
        verify(output, atLeastOnce()).collect(eq(anUser), argThat(new IdentifiedLanguageMatcher("ja")));

        final Nuplet russianTextNuplet = new Nuplet();
        russianTextNuplet.setUser(anUser);
        russianTextNuplet.setItem(new Item(ItemType.AT, "otheruser"));
        russianTextNuplet.setKey(new Key(KeyType.PLAIN_TEXT, "Абсолютно точно, что эта фраза написана на русском языке"));
        mapper.map(
                anUser,
                russianTextNuplet,
                output,
                null
        );
        verify(output, atLeastOnce()).collect(eq(anUser), argThat(new IdentifiedLanguageMatcher("ru")));
    }

    class IdentifiedLanguageMatcher extends ArgumentMatcher<Nuplet> {
        private final String lang;

        public IdentifiedLanguageMatcher(final String lang) {
            this.lang = lang;
        }

        @Override
        public boolean matches(Object argument) {
            if (argument instanceof Nuplet) {
                final Nuplet aNuplet = (Nuplet)argument;
                return lang.equals(aNuplet.getLang());
            }
            return false;
        }
    }

}
