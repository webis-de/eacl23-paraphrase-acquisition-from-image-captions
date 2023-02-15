package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.TaggedWord;
import edu.stanford.nlp.process.DocumentPreprocessor;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SentenceHeuristic implements CaptionFilterHeuristic {
    private final MaxentTagger POS_TAGGER;

    private final List<Pair<Function<String, Boolean>, Pattern>> ruleSet;
    private final Pattern replacementPattern;

    public SentenceHeuristic() {
        POS_TAGGER = new MaxentTagger(SentenceHeuristic.class.getResource("/stanford/pos-models/english-left3words-distsim.tagger").toString());

        ruleSet = new LinkedList<>();

        ruleSet.add(new ImmutablePair<>(x -> x.contains("MD"), Pattern.compile("MD( RB)? VB")));
        ruleSet.add(new ImmutablePair<>(x -> x.contains("WRB"), Pattern.compile("^(?:(?!WRB).)*?(VBP|VBZ|VBD)")));
        ruleSet.add(new ImmutablePair<>(x -> x.contains("WP"), Pattern.compile("^(?:(?!WP).)*?(VBP|VBZ|VBD)")));
        ruleSet.add(new ImmutablePair<>(x -> x.contains("WDT"), Pattern.compile("^(?:(?!WDT).)*?(VBP|VBZ|VBD)")));
        ruleSet.add(new ImmutablePair<>(x -> x.contains("IN"), Pattern.compile("^(?:(?!IN).)*?(VBP|VBZ|VBD)")));
        ruleSet.add(new ImmutablePair<>(x -> true, Pattern.compile("VBP|VBZ|VBD")));

        replacementPattern = Pattern.compile("-LRB-.+?-RRB-");
    }

    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        DocumentPreprocessor preprocessor = new DocumentPreprocessor(new StringReader(text));

        for (List<HasWord> sentence : preprocessor) {
            List<TaggedWord> taggedWords = POS_TAGGER.tagSentence(sentence);

            String patternSequence = taggedWords.stream().map(TaggedWord::tag).collect(Collectors.joining(" "));

            patternSequence = replacementPattern.matcher(patternSequence).replaceAll("");

            boolean ruleEvaluation = false;
            for (Pair<Function<String, Boolean>, Pattern> rule : ruleSet) {
                if (rule.getKey().apply(patternSequence)) {
                    ruleEvaluation = rule.getValue().matcher(patternSequence).find();
                    break;
                }
            }

            if (!ruleEvaluation) {
                return false;
            }
        }

        return true;
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.SENTENCE_HEURISTIC;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return TableCounter.F_CAPTIONS_SENTENCES;
    }

}
