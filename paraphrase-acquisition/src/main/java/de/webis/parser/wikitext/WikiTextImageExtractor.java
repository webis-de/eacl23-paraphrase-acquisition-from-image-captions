package de.webis.parser.wikitext;

import de.webis.caption_extraction.WikiTextExtractor;
import de.webis.hadoop.counter.WikiImageExtractionCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiTextImageExtractor {
    private static final Pattern FILE_SYNTAX_PATTERN = Pattern.compile("\\[\\[:?(?:[fF]ile|[iI]mage):.+]]");
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("(?:File|Image):[^|\\]]+");
    private static final Pattern IMAGE_PATTERN = Pattern.compile("^image[0-9]?$");

    private static final Pattern KEY_VAL_SPLIT = Pattern.compile("=");
    private static final Pattern ARTIFACT_SPLIT = Pattern.compile("\\{\\{!}}");
    private static final Pattern XML_TAG_PATTERN = Pattern.compile("<.+?>|</.+?>|<.+?/>");

    private final Matcher fileSyntaxMatcher;
    private final Matcher fileNameMatcher;

    private final Deque<Integer> squarePosStack;
    private final Deque<Integer> bracePosStack;
    private final Queue<String> relevantTokens;

    private final ImageReferenceWritable outReference;

    private final Map<String, String> currentImageNames;
    private final Map<String, String> currentImageCaptions;
    private final Map<String, String> currentAltTexts;
    private final StringBuilder tagBuilder;

    private String wikiText;
    private int pos;
    private String currentToken;
    private boolean articleCounted;

    private final Counter articlesWithInfoboxes;
    private final Counter referencesInBody;
    private final Counter referencesInTemplates;
    private final Counter referencesInInfoboxes;
    private final Counter infoboxesWithReferences;
    private final Counter infoboxesWithCaption;

    public WikiTextImageExtractor(Mapper.Context context) {
        fileSyntaxMatcher = FILE_SYNTAX_PATTERN.matcher("");
        fileNameMatcher = FILE_NAME_PATTERN.matcher("");

        wikiText = null;

        squarePosStack = new ArrayDeque<>();
        bracePosStack = new ArrayDeque<>();

        relevantTokens = new ArrayDeque<>();

        pos = 0;
        currentToken = null;
        outReference = new ImageReferenceWritable();

        currentImageNames = new HashMap<>();
        currentImageCaptions = new HashMap<>();
        currentAltTexts = new HashMap<>();
        tagBuilder = new StringBuilder();

        if (context != null) {
            articlesWithInfoboxes = context.getCounter(WikiImageExtractionCounter.ARTICLES_WITH_INFOBOXES);

            referencesInBody = context.getCounter(WikiImageExtractionCounter.REFERENCES_IN_BODY);
            referencesInTemplates = context.getCounter(WikiImageExtractionCounter.REFERENCES_IN_TEMPLATES);
            referencesInInfoboxes = context.getCounter(WikiImageExtractionCounter.REFERENCES_IN_INFOBOXES);

            infoboxesWithReferences = context.getCounter(WikiImageExtractionCounter.INFOBOXES_WITH_REFERENCES);
            infoboxesWithCaption = context.getCounter(WikiImageExtractionCounter.INFOBOXES_WITH_REFERENCES_AND_CAPTIONS);
        } else {
            articlesWithInfoboxes = new Counters.Counter();

            referencesInBody = new Counters.Counter();
            referencesInTemplates = new Counters.Counter();
            referencesInInfoboxes = new Counters.Counter();

            infoboxesWithReferences = new Counters.Counter();
            infoboxesWithCaption = new Counters.Counter();
        }

        articleCounted = false;
    }

    public boolean next() {
        if (!relevantTokens.isEmpty()) {
            return true;
        }

        char currentChar;
        char nextChar;

        for (; pos < wikiText.length() - 1; pos++) {
            currentChar = wikiText.charAt(pos);
            nextChar = wikiText.charAt(pos + 1);

            if (currentChar == '[' && nextChar == '[') {
                squarePosStack.push(pos);
                pos++;
            } else if (currentChar == '{' && nextChar == '{') {
                bracePosStack.push(pos);
                pos++;
            } else if (currentChar == ']' && nextChar == ']') {
                if (!squarePosStack.isEmpty()) {
                    currentToken = wikiText.substring(squarePosStack.pop(), pos + 2);

                    boolean consider = handleSquareToken();

                    if (consider) {
                        pos++;
                        return true;
                    }
                }

                pos++;
            } else if (currentChar == '}' && nextChar == '}') {
                if (!bracePosStack.isEmpty()) {
                    currentToken = wikiText.substring(bracePosStack.pop(), pos + 2);

                    boolean consider = handleBraceToken();

                    if (consider) {
                        pos++;
                        return true;
                    }
                }

                pos++;
            }
        }

        return false;
    }

    public String getCurrent() {
        return relevantTokens.poll();
    }

    public void reset(String wikiText) {
        this.wikiText = wikiText;

        squarePosStack.clear();
        bracePosStack.clear();
        relevantTokens.clear();
        currentToken = null;

        pos = 0;
        outReference.clear();

        currentImageNames.clear();
        currentImageCaptions.clear();
        tagBuilder.delete(0, tagBuilder.length());
        articleCounted = false;
    }

    private boolean handleSquareToken() {
        fileSyntaxMatcher.reset(currentToken);
        if (fileSyntaxMatcher.matches()) {
            fileNameMatcher.reset(currentToken);

            if (fileNameMatcher.find()) {
                referencesInBody.increment(1L);

                relevantTokens.add(currentToken);
                return true;
            }
        }

        return false;
    }

    private boolean handleBraceToken() {
        currentImageNames.clear();
        currentImageCaptions.clear();
        currentAltTexts.clear();

        Counter referenceCounter = null;
        boolean templateReferenceCounted = false;
        boolean templateCaptionCounted = false;
        boolean isInfobox = false;

        currentToken = ARTIFACT_SPLIT.matcher(currentToken).replaceAll("|");

        if (currentToken.length() < 5) {
            return false;
        }

        List<String> templateComponents;
        try {
            templateComponents = WikiTextExtractor.tokenize(currentToken);
        } catch (StringIndexOutOfBoundsException e) {
            System.out.println(currentToken);
            throw e;
        }


        if (!templateComponents.isEmpty()) {
            if (templateComponents.get(0).toLowerCase().startsWith("infobox")) {
                if (!articleCounted)
                    articlesWithInfoboxes.increment(1L);

                referenceCounter = referencesInInfoboxes;
                isInfobox = true;
            }
        }

        if (referenceCounter == null)
            referenceCounter = referencesInTemplates;

        if (!currentToken.contains("image"))
            return false;

        for (String comp : templateComponents) {
            String[] keyVals = KEY_VAL_SPLIT.split(comp);

            if (keyVals.length > 0) {
                String key = keyVals[0].trim().toLowerCase();
                key = XML_TAG_PATTERN.matcher(key).replaceAll("");
                String value = String.join("", Arrays.copyOfRange(keyVals, 1, keyVals.length)).trim();

                if (IMAGE_PATTERN.matcher(key).matches()) {
                    String imageId = key.replace("image", "");
                    if (FILE_SYNTAX_PATTERN.matcher(value).matches()) {
                        referencesInBody.increment(-1L);

                        referenceCounter.increment(1L);
                    } else {
                        if (!value.isEmpty()) {
                            currentImageNames.put(imageId, value);
                        }
                    }

                    if (!value.isEmpty()) {
                        if (!templateReferenceCounted && isInfobox) {
                            infoboxesWithReferences.increment(1L);
                            templateReferenceCounted = true;
                        }
                    }
                } else if (key.startsWith("caption")) {
                    String captionId = key.replace("caption", "");

                    if (!value.isEmpty()) {
                        currentImageCaptions.put(captionId, value);

                        if (!templateCaptionCounted && isInfobox) {
                            infoboxesWithCaption.increment(1L);
                            templateCaptionCounted = true;
                        }
                    }
                } else if (key.startsWith("alt")) {
                    String altId = key.replace("alt", "");

                    if (!value.isEmpty()) {
                        currentAltTexts.put(altId, value);
                    }
                }
            }
        }

        constructFileTags(referenceCounter);
        return !relevantTokens.isEmpty();
    }

    private void constructFileTags(Counter referenceCounter) {
        for (Map.Entry<String, String> imageName : currentImageNames.entrySet()) {
            tagBuilder.append("[[");

            if (!imageName.getValue().startsWith("File:") && !imageName.getValue().startsWith("Image:")) {
                tagBuilder.append("File:");
            }

            tagBuilder.append(imageName.getValue());

            String alt = currentAltTexts.getOrDefault(imageName.getKey(), null);
            if (alt != null) {
                tagBuilder.append("|").append("alt=").append(alt);
            }

            String caption = currentImageCaptions.getOrDefault(imageName.getKey(), null);
            if (caption != null) {
                tagBuilder.append("|").append(caption);
            }

            tagBuilder.append("]]");

            fileSyntaxMatcher.reset(tagBuilder.toString());
            if (fileSyntaxMatcher.matches()) {
                relevantTokens.add(tagBuilder.toString());

                referenceCounter.increment(1L);
            }

            tagBuilder.delete(0, tagBuilder.length());
        }
    }
}
