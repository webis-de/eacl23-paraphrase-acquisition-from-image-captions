package de.webis.nlp.tokenizer;

import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.*;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StanfordWordTokenizer implements Tokenizer {
    private final static Pattern NON_WORD_TOKEN_PATTERN = Pattern.compile("\\W+");

    private final StanfordCoreNLP pipeline;

    public StanfordWordTokenizer() {
        Properties properties = new Properties();
        properties.setProperty("annotators", "tokenize");
        properties.setProperty("tokenize", "PTBTokenizer");
        properties.setProperty("tokenize.options", "" +
                "invertible=false," +
                "normalizeOtherBrackets=false," +
                "splitHyphenated=false");


        pipeline = new StanfordCoreNLP(properties);
    }

    @Override
    public List<CoreLabel> tokenize(String text) {
        CoreDocument doc = new CoreDocument(text);
        pipeline.annotate(doc);

        List<CoreLabel> tokens = doc.tokens();
        return tokens.stream().filter(t -> !NON_WORD_TOKEN_PATTERN.matcher(t.word()).matches()).collect(Collectors.toList());
    }

    public static void main(String[] args) {
        Tokenizer tokenizer = new StanfordWordTokenizer();
        System.out.println(tokenizer.tokenize("} {{{lived|}}}"));
        System.out.println(tokenizer.tokenize("Man-reading-Emancipation-Proclamation-1863"));
    }
}
