package de.webis.nlp.similarity;

import de.webis.nlp.tokenizer.NGramTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.*;

public class BleuMetric implements ParaphraseSimilarity {
    private final int maxN;
    private final Tokenizer nGramTokenizer;

    public BleuMetric() {
        maxN = 4;
        nGramTokenizer = new NGramTokenizer(maxN);
    }

    @Override
    public double score(String first, String second) {
        List<CoreLabel> firstTokens = nGramTokenizer.tokenize(first);
        List<CoreLabel> secondTokens = nGramTokenizer.tokenize(second);

        double score = 0.0;

        for (int n = 1; n <= maxN; n++) {
            Map<String, Integer> firstNGrams = countNGrams(firstTokens, n);
            Map<String, Integer> secondNGrams = countNGrams(secondTokens, n);

            Set<String> coOccuringNGrams = new HashSet<>(firstNGrams.keySet());
            coOccuringNGrams.retainAll(secondNGrams.keySet());

            int maxCount;

            if (firstTokens.size() <= secondTokens.size()) {
                maxCount = firstNGrams.size();
            } else {
                maxCount = secondNGrams.size();
            }

            double modifiedPrecision = (double) coOccuringNGrams.size() / maxCount;

            if (modifiedPrecision > 0)
                score += (1.0 / maxN) * Math.log(modifiedPrecision);
        }
//        if(score == 0){
//            return 0.0;
//        }

        score = Math.exp(score);

        return score;
    }

    private Map<String, Integer> countNGrams(List<CoreLabel> tokens, int n) {
        Map<String, Integer> nGrams = new HashMap<>();

        for (CoreLabel label : tokens) {
            if (label.tag().equals(n + "-gram")) {
                String ngram = label.value().toLowerCase();

                nGrams.putIfAbsent(ngram, 0);
                nGrams.put(ngram, nGrams.get(ngram) + 1);
            }
        }

        return nGrams;
    }

    public static void main(String[] args) {
        ParaphraseSimilarity similarity = new BleuMetric();
        System.out.println(similarity.score(
                "An illustration of tight gas compared to other types of gas deposits.",
                "A diagram showing the geologic sources of alkane hydrocarbon gases which accompany the extraction of coal and crude oil, or which are themselves the target of extraction."));

        System.out.println(similarity.score(
                "This text has nothing to do with the other", "No word is shared."));
    }
}
