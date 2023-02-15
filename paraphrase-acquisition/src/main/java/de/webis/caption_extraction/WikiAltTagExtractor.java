package de.webis.caption_extraction;

import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extractor that extracts alternative texts from Wikitext image tags
 */
public class WikiAltTagExtractor implements CaptionExtractor {
    private final static Pattern ALT_TAG_PATTERN = Pattern.compile("(?<=^ ?alt ?=).*$");

    /**
     * Extract caption from image tag.
     *
     * @param imageTag Wikitext image tag
     * @return Alternative text
     */
    @Nullable
    @Override
    public String extract(final String imageTag) {
        List<String> tokens = WikiTextExtractor.tokenize(imageTag);

        for(String token: tokens){
            token = token.trim();
            Matcher matcher = ALT_TAG_PATTERN.matcher(token);

            if(matcher.find()){
                return matcher.group(0);
            }
        }

        return null;
    }

    /**
     * Get the type of the caption this algorithm extracts.
     *
     * @return CaptionType.WIKI_TEXT_ALT_TAG
     */
    @Override
    public CaptionType getCaptionType() {
        return CaptionType.WIKI_TEXT_ALT_TAG;
    }
}
