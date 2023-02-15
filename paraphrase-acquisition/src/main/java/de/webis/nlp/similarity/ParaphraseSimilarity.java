package de.webis.nlp.similarity;

public interface ParaphraseSimilarity {
    double score(final String first, final String second);
}
