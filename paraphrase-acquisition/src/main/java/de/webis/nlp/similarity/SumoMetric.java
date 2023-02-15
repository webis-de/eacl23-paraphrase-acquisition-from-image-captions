package de.webis.nlp.similarity;

import de.webis.nlp.tokenizer.NGramTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;
import java.util.stream.Collectors;

public class SumoMetric implements ParaphraseSimilarity {
    private final Tokenizer nGramTokenizer;

    public SumoMetric() {
        nGramTokenizer = new NGramTokenizer(1);
    }

    @Override
    public double score(String first, String second) {
        List<CoreLabel> firstTokens = nGramTokenizer.tokenize(first);
        List<CoreLabel> secondTokens = nGramTokenizer.tokenize(second);

        int lambda = countLinks(firstTokens, secondTokens);
        int x, y;

        x = Math.max(firstTokens.size(), secondTokens.size());
        y = Math.min(firstTokens.size(), secondTokens.size());

        double alpha = 0.5;
        double beta = 0.5;

        double sumo = alpha * log2((double) x / lambda) + beta * log2((double) y / lambda);

        if (sumo >= 1.0) {
            sumo = Math.exp(-3 * sumo);
        }
        return sumo;
    }

    private int countLinks(final List<CoreLabel> first, final List<CoreLabel> second) {
        int numLinks = 0;
        List<String> firstUniGrams = first.stream().map(t -> t.value().toLowerCase()).collect(Collectors.toList());
        List<String> secondUniGrams = second.stream().map(t -> t.value().toLowerCase()).collect(Collectors.toList());
        if (first.size() >= second.size()) {
            for (String label : firstUniGrams) {
                if (secondUniGrams.contains(label)) {
                    numLinks++;
                    secondUniGrams.remove(label);
                }
            }
        } else {
            for (String label : secondUniGrams) {
                if (firstUniGrams.contains(label)) {
                    numLinks++;
                    firstUniGrams.remove(label);
                }
            }
        }

        return numLinks;
    }

    private double log2(double val) {
        return Math.log10(val) / Math.log10(2);
    }

    public static void main(String[] args) {
        ParaphraseSimilarity similarity = new SumoMetric();
        System.out.println(similarity.score(
                "An illustration of tight gas compared to other types of gas deposits.",
                "A diagram showing the geologic sources of alkane hydrocarbon gases which accompany the extraction of coal and crude oil, or which are themselves the target of extraction."));

        System.out.println(similarity.score(
                "These are equal strings.",
                "these are equal strings."));

        System.out.println(similarity.score(
                "on july troy is expected to be sentenced to life in prison without parole", "troy faces life in prison without parole at his july sentencing"
        ));
    }
}
