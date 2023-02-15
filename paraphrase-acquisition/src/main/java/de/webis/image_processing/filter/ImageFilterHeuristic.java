package de.webis.image_processing.filter;

import de.webis.hadoop.counter.TableCounter;
import org.bson.Document;

public interface ImageFilterHeuristic {
    boolean accept(Document imageDocument);

    TableCounter getPassCounterCaptions();

    TableCounter getPassCounterReferences();
}
