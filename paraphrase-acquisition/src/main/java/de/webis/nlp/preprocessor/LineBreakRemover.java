package de.webis.nlp.preprocessor;

import java.util.regex.Pattern;

public class LineBreakRemover implements Preprocessor {
    private final static Pattern LINE_BREAK_PATTERN = Pattern.compile("\n");

    @Override
    public String process(String text) {
        return LINE_BREAK_PATTERN.matcher(text).replaceAll(" ");
    }
}
