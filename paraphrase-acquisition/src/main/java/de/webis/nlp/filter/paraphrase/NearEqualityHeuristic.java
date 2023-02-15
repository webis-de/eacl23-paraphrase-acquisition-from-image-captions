package de.webis.nlp.filter.paraphrase;

import de.webis.hadoop.counter.ParaphraseFilterCounter;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class NearEqualityHeuristic implements ParaphraseFilterHeuristic {
    @Override
    public boolean accept(String firstText, List<CoreLabel> firstTokens, String secondText, List<CoreLabel> secondTokens) {
        return false;
    }

    @Override
    public boolean accept(ParaphraseWritable paraphraseWritable) {
        String first = paraphraseWritable.getFirst().replaceAll("\\(.*?\\)", "").replaceAll("[\\W]", "").toLowerCase();
        String second = paraphraseWritable.getSecond().replaceAll("\\(.*?\\)", "").replaceAll("[\\W]", "").toLowerCase();

        boolean equal = first.equals(second);
        equal |= first.contains(second);
        equal |= second.contains(first);

        return !equal;
    }

    @Override
    public ParaphraseFilterCounter getCounterType() {
        return ParaphraseFilterCounter.NEAR_EQUALITY;
    }
}
