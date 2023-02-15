package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class EmptyHeuristic implements CaptionFilterHeuristic {
    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        return !text.isEmpty();
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.EMPTY;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return TableCounter.D_CAPTIONS_NON_EMPTY;
    }
}
