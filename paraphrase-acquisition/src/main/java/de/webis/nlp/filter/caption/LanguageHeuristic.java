package de.webis.nlp.filter.caption;

import com.github.pemistahl.lingua.api.Language;
import com.github.pemistahl.lingua.api.LanguageDetector;
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder;
import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class LanguageHeuristic implements CaptionFilterHeuristic {
    private final LanguageDetector languageDetector;

    public LanguageHeuristic() {
        languageDetector = LanguageDetectorBuilder.fromAllSpokenLanguages().build();
    }

    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        return languageDetector.detectLanguageOf(text) == Language.ENGLISH;
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.LANGUAGE;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return null;
    }
}
