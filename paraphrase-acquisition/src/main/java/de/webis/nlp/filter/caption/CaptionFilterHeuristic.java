package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public interface CaptionFilterHeuristic {
    boolean accept(String text, List<CoreLabel> tokens);

    CaptionFilterCounter getCounterType();

    TableCounter getPassCounterCaptions();
}
