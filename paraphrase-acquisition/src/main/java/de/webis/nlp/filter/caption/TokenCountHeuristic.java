package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class TokenCountHeuristic implements CaptionFilterHeuristic {
    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        return tokens.size() >= 6;
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.TOKEN_COUNT;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return TableCounter.E_CAPTIONS_LONG_ENOUGH;
    }
}
