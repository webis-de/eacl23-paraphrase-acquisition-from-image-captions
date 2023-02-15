package de.webis.nlp.tokenizer;

import edu.stanford.nlp.ling.CoreLabel;

import java.util.LinkedList;
import java.util.List;

public class NGramTokenizer implements Tokenizer {
    private final int maxN;
    private final Tokenizer wordTokenizer;

    public NGramTokenizer(int maxN) {
        this.maxN = maxN;

        wordTokenizer = new StanfordWordTokenizer();
    }

    @Override
    public List<CoreLabel> tokenize(String text) {
        final List<CoreLabel> words = wordTokenizer.tokenize(text);
        final List<CoreLabel> nGrams = new LinkedList<>();

        for (int i = 1; i <= maxN; i++) {
            for (int j = 0; j < words.size() - i + 1; j++) {
                CoreLabel nGram = new CoreLabel(words.get(j));
                nGram.setTag(i + "-gram");
                for (int k = 1; k < i; k++) {
                    nGram.setValue(nGram.value() + " " + words.get(j + k));
                }

                nGrams.add(nGram);
            }
        }

        return nGrams;
    }
}
