package de.webis.nlp.filter.paraphrase;

import de.webis.hadoop.counter.ParaphraseFilterCounter;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class CommonWordHeuristic implements ParaphraseFilterHeuristic {
    private final static Pattern NON_WORD_PATTERN = Pattern.compile("\\W+");

    @Override
    public boolean accept(String firstText, List<CoreLabel> firstTokens,
                          String secondText, List<CoreLabel> secondTokens) {
        Set<String> firstWords = new HashSet<>();
        Set<String> secondWords = new HashSet<>();

        for (CoreLabel label : firstTokens) {
            String word = label.value().toLowerCase();

            if (!NON_WORD_PATTERN.matcher(word).matches()) {
                firstWords.add(word);
            }

        }

        for (CoreLabel label : secondTokens) {
            String word = label.value().toLowerCase();

            if (!NON_WORD_PATTERN.matcher(word).matches()) {
                secondWords.add(word);
            }

        }

        firstWords.retainAll(secondWords);

        return firstWords.size() >= 3;
    }

    @Override
    public boolean accept(ParaphraseWritable paraphraseWritable) {
        return false;
    }

    @Override
    public ParaphraseFilterCounter getCounterType() {
        return ParaphraseFilterCounter.COMMON_WORDS;
    }
}
