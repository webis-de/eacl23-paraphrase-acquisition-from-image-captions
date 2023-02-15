package de.webis.caption_extraction;

import org.jetbrains.annotations.Nullable;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

/**
 * Extractor that extracts alternative texts from HTML image tags
 */
public class HtmlAltTagExtractor implements CaptionExtractor {

    /**
     * Extract caption from image tag.
     *
     * @param imageTag HTML img tag
     * @return Alternative text
     */
    @Override
    @Nullable
    public String extract(final String imageTag) {
        final Document doc = Jsoup.parse(imageTag);
        final Element element = doc.select("img").first();

        if (element != null)
            if (element.hasAttr("alt"))
                return element.attr("alt");
            else {
                return null;
            }
        else {
            System.err.println("ERROR: Extracting caption from \"" + imageTag);
            return null;
        }
    }

    /**
     * Get the type of the caption this algorithm extracts.
     *
     * @return CaptionType.HTML_ALT_TAG
     */
    @Override
    public CaptionType getCaptionType() {
        return CaptionType.HTML_ALT_TAG;
    }
}
