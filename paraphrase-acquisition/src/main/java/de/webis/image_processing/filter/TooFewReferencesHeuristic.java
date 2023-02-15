package de.webis.image_processing.filter;

import de.webis.hadoop.counter.TableCounter;
import org.bson.Document;

public class TooFewReferencesHeuristic implements ImageFilterHeuristic {
    @Override
    public boolean accept(Document imageDocument) {
        int numReferences = imageDocument.getInteger("num_references");

        return numReferences >= 2;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return TableCounter.B_CAPTIONS_NUM_REFERENCES;
    }

    @Override
    public TableCounter getPassCounterReferences() {
        return TableCounter.B_REFERENCES_NUM_REFERENCES;
    }
}
