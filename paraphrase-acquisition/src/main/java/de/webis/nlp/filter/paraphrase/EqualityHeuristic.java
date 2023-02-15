package de.webis.nlp.filter.paraphrase;

import de.webis.hadoop.counter.ParaphraseFilterCounter;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class EqualityHeuristic implements ParaphraseFilterHeuristic {
    @Override
    public boolean accept(String firstText, List<CoreLabel> firstTokens, String secondText, List<CoreLabel> secondTokens) {
        return !firstText.equalsIgnoreCase(secondText);
    }

    @Override
    public boolean accept(ParaphraseWritable paraphraseWritable) {
        return !paraphraseWritable.getFirst().equalsIgnoreCase(paraphraseWritable.getSecond());
    }

    @Override
    public ParaphraseFilterCounter getCounterType() {
        return ParaphraseFilterCounter.EQUALITY;
    }
}
