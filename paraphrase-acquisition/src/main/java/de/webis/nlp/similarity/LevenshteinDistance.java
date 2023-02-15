package de.webis.nlp.similarity;

import de.webis.nlp.tokenizer.NGramTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public class LevenshteinDistance implements ParaphraseSimilarity {
    private final Tokenizer tokenizer;

    public LevenshteinDistance() {
        tokenizer = new NGramTokenizer(1);
    }

    @Override
    public double score(String first, String second) {
        List<CoreLabel> firstTokens = tokenizer.tokenize(first.toLowerCase());
        List<CoreLabel> secondTokens = tokenizer.tokenize(second.toLowerCase());

        return (double) compute(firstTokens, secondTokens) / Math.max(firstTokens.size(), secondTokens.size());
    }

    private int compute(List<CoreLabel> lhs, List<CoreLabel> rhs) {
        int len0 = lhs.size() + 1;
        int len1 = rhs.size() + 1;

        // the array of distances
        int[] cost = new int[len0];
        int[] newcost = new int[len0];

        // initial cost of skipping prefix in String s0
        for (int i = 0; i < len0; i++) cost[i] = i;

        // dynamically computing the array of distances

        // transformation cost for each letter in s1
        for (int j = 1; j < len1; j++) {
            // initial cost of skipping prefix in String s1
            newcost[0] = j;

            // transformation cost for each letter in s0
            for (int i = 1; i < len0; i++) {
                // matching current letters in both strings
                int match = (lhs.get(i - 1).word().equals(rhs.get(j - 1).word())) ? 0 : 1;

                // computing cost for each transformation
                int cost_replace = cost[i - 1] + match;
                int cost_insert = cost[i] + 1;
                int cost_delete = newcost[i - 1] + 1;

                // keep minimum cost
                newcost[i] = Math.min(Math.min(cost_insert, cost_delete), cost_replace);
            }

            // swap cost/newcost arrays
            int[] swap = cost;
            cost = newcost;
            newcost = swap;
        }

        // the distance is the cost for transforming all letters in both strings
        return cost[len0 - 1];
    }
}
