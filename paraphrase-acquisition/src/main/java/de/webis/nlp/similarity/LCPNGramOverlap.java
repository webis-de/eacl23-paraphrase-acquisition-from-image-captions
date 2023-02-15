package de.webis.nlp.similarity;

import de.webis.nlp.tokenizer.NGramTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LCPNGramOverlap implements ParaphraseSimilarity {
    private final int maxN;
    private final Tokenizer nGramTokenizer;

    public LCPNGramOverlap() {
        maxN = 4;
        nGramTokenizer = new NGramTokenizer(maxN);
    }

    @Override
    public double score(String first, String second) {
        List<CoreLabel> firstTokens = nGramTokenizer.tokenize(first);
        List<CoreLabel> secondTokens = nGramTokenizer.tokenize(second);

        double max = 0.0;
        double score;

        for (int n = 1; n <= maxN; n++) {
            Set<String> firstNGrams = extractNGrams(firstTokens, n);
            Set<String> secondNGrams = extractNGrams(secondTokens, n);

            Set<String> matchingNgrams = new HashSet<>(firstNGrams);
            matchingNgrams.retainAll(secondNGrams);

            if (firstTokens.size() <= secondTokens.size())
                score = (double) (matchingNgrams.size()) / (double) (firstNGrams.size());
            else
                score = (double) (matchingNgrams.size()) / (double) (secondNGrams.size());

            if (score > max) {
                max = score;
            }
        }

        return max;
    }

    private Set<String> extractNGrams(List<CoreLabel> tokens, int n) {
        Set<String> nGrams = new HashSet<>();

        for (CoreLabel label : tokens) {
            if (label.tag().equals(n + "-gram")) {
                nGrams.add(label.value().toLowerCase());
            }
        }

        return nGrams;
    }

    public static void main(String[] args) {
        ParaphraseSimilarity similarity = new LCPNGramOverlap();
        System.out.println(similarity.score(
                "An illustration of tight gas compared to other types of gas deposits.",
                "A diagram showing the geologic sources of alkane hydrocarbon gases which accompany the extraction of coal and crude oil, or which are themselves the target of extraction."));

        System.out.println(similarity.score(
                "These are equal strings.",
                "these are similar strings."));
    }
}
