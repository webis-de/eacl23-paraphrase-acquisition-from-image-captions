package de.webis.nlp.preprocessor;

import java.util.regex.Pattern;

public class NonPrintableCharRemover implements Preprocessor {
    private final static Pattern NON_PRINTABLE_PATTERN = Pattern.compile("[^\\p{Print}]+");

    @Override
    public String process(String text) {
        return NON_PRINTABLE_PATTERN.matcher(text).replaceAll(" ");
    }
}
