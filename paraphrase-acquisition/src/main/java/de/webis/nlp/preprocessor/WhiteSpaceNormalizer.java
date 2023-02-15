package de.webis.nlp.preprocessor;

import java.util.regex.Pattern;

public class WhiteSpaceNormalizer implements Preprocessor {
    private static final Pattern MULTI_WHITESPACE_PATTERN = Pattern.compile("\\s+");

    @Override
    public String process(String text) {
        return MULTI_WHITESPACE_PATTERN.matcher(text).replaceAll(" ").trim();
    }
}
