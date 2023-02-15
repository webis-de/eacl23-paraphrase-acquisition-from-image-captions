package de.webis.caption_extraction;

/**
 * Interface for algorithms that extract captions from different markdown languages.
 */
public interface CaptionExtractor {

    /**
     * Extract caption from image tag.
     *
     * @param imageTag Markdown tag to encode images
     * @return Extracted caption
     */
    String extract(final String imageTag);

    /**
     * Get the type of the caption this algorithm extracts.
     *
     * @return caption type of extracted captions
     */
    CaptionType getCaptionType();
}
