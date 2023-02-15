package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;

import java.util.List;

public class VerbHeuristic implements CaptionFilterHeuristic{
    private final MaxentTagger POS_TAGGER;

    public VerbHeuristic(){
        POS_TAGGER = new MaxentTagger(VerbHeuristic.class.getResource("/stanford/pos-models/english-left3words-distsim.tagger").toString());
    }

    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        POS_TAGGER.tagCoreLabels(tokens);

        for(CoreLabel label: tokens){
            if(label.tag().startsWith("VB")){
                return false;
            }
        }


        return true;
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.VERB_HEURISTIC;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return TableCounter.F_CAPTIONS_VERBS;
    }
}
