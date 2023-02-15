package de.webis.nlp.filter.paraphrase;

import de.webis.hadoop.counter.ParaphraseFilterCounter;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public interface ParaphraseFilterHeuristic {
    boolean accept(String firstText, List<CoreLabel> firstTokens,
                   String secondText, List<CoreLabel> secondTokens);

    boolean accept(ParaphraseWritable paraphraseWritable);

    ParaphraseFilterCounter getCounterType();
}
