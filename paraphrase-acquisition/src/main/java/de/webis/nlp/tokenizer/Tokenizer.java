package de.webis.nlp.tokenizer;

import edu.stanford.nlp.ling.CoreLabel;

import java.util.List;

public interface Tokenizer {
    List<CoreLabel> tokenize(String text);
}
