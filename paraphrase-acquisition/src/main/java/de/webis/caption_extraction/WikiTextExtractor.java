package de.webis.caption_extraction;

import de.webis.nlp.preprocessor.Preprocessor;
import de.webis.nlp.preprocessor.WhiteSpaceNormalizer;
import de.webis.parser.wikitext.WikiTextImageExtractor;
import org.apache.commons.lang.StringEscapeUtils;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extractor that extracts captions from Wikitext image tags
 */
public class WikiTextExtractor implements CaptionExtractor {
    private final Map<String, Function<String[], String>> templateReplacementRules;

    private static final List<Pattern> NON_CAPTION_PATTERN = Arrays.asList(
            Pattern.compile("^:?(File|Image):.*$"),
            Pattern.compile("^thumb(nail)?\\s*?(=.*?)?$"),
            Pattern.compile("^frame(d|less)?$"),
            Pattern.compile("^border$"),
            Pattern.compile("^(right|left|center|centre|none)$"),
            Pattern.compile("^upright(\\s*?=\\s*?[0-9.]+)?$"),
            Pattern.compile("^[0-9x]+\\s*?px$"),
            Pattern.compile("^link\\s*?=.*?$"),
            Pattern.compile("^alt\\s*?=.*?$"),
            Pattern.compile("^class\\s*?=.*?$"),
            Pattern.compile("^page\\s*?=[0-9]+$"),
            Pattern.compile("^lang\\s*?=.*?$")
    );

    private static final Pattern CITATION_PATTERN = Pattern.compile("\\{\\{[cC]ite.*?}}");
    private static final Pattern WIKILINK_PATTERN = Pattern.compile("\\[\\[[^|]+?]]");
    private static final Pattern NAMED_WIKILINK_PATTERN = Pattern.compile("\\[\\[.+?\\|.+?]]");
    private static final Pattern EXTERNAL_LINK_PATTERN = Pattern.compile("\\[http.*?]");
    private static final Pattern XML_TAG_PATTERN = Pattern.compile("<.+?>|</.+?>|<.+?/>");
    private static final Pattern REF_TAG_PATTERN = Pattern.compile("<ref.*</ref>");
    private static final Pattern ITALIC_PATTERN = Pattern.compile("''");
    private static final Pattern BOLD_PATTERN = Pattern.compile("'''");

    private static final Preprocessor WHITESPACE_NORMALIZER = new WhiteSpaceNormalizer();

    private final Deque<Integer> bracketPosStack;

    public WikiTextExtractor() {
        bracketPosStack = new ArrayDeque<>();
        templateReplacementRules = new HashMap<>();

        final Function<String[], String> shipFunction = s -> {
            if (s.length == 4) {
                return String.join(" ", Arrays.copyOfRange(s, 1, 3)) + " (" + s[3] + ")";
            } else if (s.length == 5) {
                String formatLabel = s[3];

                switch (formatLabel) {
                    case "1":
                        return s[3];
                    case "2":
                        return s[2];
                    case "3":
                        return s[2] + " (" + s[3] + ")";
                    case "5":
                        return s[1] + " " + s[3];
                    case "6":
                        return s[1] + " " + s[2];
                    default:
                        return String.join(" ", Arrays.copyOfRange(s, 1, 3)) + " (" + s[3] + ")";
                }
            }

            return String.join(" ", s);
        };



        final Function<String[], String> ussFunction = s -> {
            if (s.length == 3) {
                return String.join(" ", Arrays.copyOfRange(s, 0, 2)) + " (" + s[2] + ")";
            } else if (s.length == 4) {
                String formatLabel = s[3];

                switch (formatLabel) {
                    case "1":
                        return s[2];
                    case "2":
                        return s[1];
                    case "3":
                        return s[1] + " (" + s[2] + ")";
                    case "5":
                        return s[0] + " " + s[2];
                    case "6":
                        return s[0] + " " + s[1];
                    default:
                        return String.join(" ", Arrays.copyOfRange(s, 0, 2)) + " (" + s[2] + ")";
                }
            }

            return String.join(" ", s);
        };

        final Function<String[], String> mvFunction = s -> {
            if (s.length == 3) {
                return String.join(" ", Arrays.copyOfRange(s, 0, 2)) + " (" + s[2] + ")";
            } else if (s.length == 4) {
                String formatLabel = s[3];

                switch (formatLabel) {
                    case "1":
                        return s[2];
                    case "2":
                        return s[1];
                    case "3":
                        return s[1] + " (" + s[2] + ")";
                    case "5":
                        return s[0] + " " + s[2];
                    case "6":
                        return s[0] + " " + s[1];
                    default:
                        return String.join(" ", Arrays.copyOfRange(s, 0, 2)) + " (" + s[2] + ")";
                }
            }

            return String.join(" ", s);
        };

        templateReplacementRules.put(
                "sfn",
                s -> "");
        templateReplacementRules.put(
                "=",
                s -> s[0]);
        templateReplacementRules.put(
                "uss",
                ussFunction);
        templateReplacementRules.put(
                "hms",
                ussFunction);
        templateReplacementRules.put(
                "mv",
                mvFunction);
        templateReplacementRules.put(
                "nycs",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "angbr",
                s -> s.length > 1 ? "<" + s[1] + ">" : "");
        templateReplacementRules.put(
                "infix",
                s -> s.length > 1 ? "<" + s[1] + ">" : "");
        templateReplacementRules.put(
                "grapheme",
                s -> s.length > 1 ? "<" + s[1] + ">" : "");
        templateReplacementRules.put(
                "ipablink",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "circa",
                s -> s.length > 1 ? s[0] + " " + s[1] : s[0]);
        templateReplacementRules.put(
                "interlanguage link",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "ill",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "convert",
                s -> s.length > 2 ? s[1] + s[2] : s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "cvt",
                s -> s.length > 2 ? s[1] + s[2] : s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "legend",
                s -> " * " + s[s.length - 1]);
        templateReplacementRules.put(
                "spaces",
                s -> " ");
        templateReplacementRules.put(
                "nbsp",
                s -> " ");
        templateReplacementRules.put(
                "&nbsp",
                s -> " ");
        templateReplacementRules.put(
                "nbs",
                s -> " ");
        templateReplacementRules.put(
                "nbsp;",
                s -> " ");
        templateReplacementRules.put(
                "space",
                s -> " ");
        templateReplacementRules.put(
                "spcs",
                s -> " ");
        templateReplacementRules.put(
                "ov",
                s -> s.length > 1 ? s[0] + "-" + s[1] : "");
        templateReplacementRules.put(
                "nowrap",
                s -> s.length > 1 ? String.join("", Arrays.copyOfRange(s, 1, s.length)) : "");
        templateReplacementRules.put(
                "nowr",
                templateReplacementRules.get("nowrap"));
        templateReplacementRules.put(
                "'s",
                s -> "'s");
        templateReplacementRules.put(
                "rp",
                s -> "");
        templateReplacementRules.put(
                "e",
                s -> s.length > 1 ? "e^(" + s[1] + ")" : "e");
        templateReplacementRules.put(
                "credit",
                s -> "");
        templateReplacementRules.put(
                "ordered list",
                s -> s.length > 1 ? String.join(", ", Arrays.copyOfRange(s, 1, s.length)) : "");
        templateReplacementRules.put(
                "lang",
                s -> s.length > 0 ? s[s.length - 1] : "");
        templateReplacementRules.put(
                "nihongo",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "efn",
                s -> "");
        templateReplacementRules.put(
                "code",
                s -> s.length > 0 ? s[s.length - 1] : "");
        templateReplacementRules.put(
                "sfnp",
                s -> "");
        templateReplacementRules.put(
                "notetag",
                s -> "");
        templateReplacementRules.put(
                "center",
                s -> s.length > 1 ? s[1] : "");
        templateReplacementRules.put(
                "math",
                s -> s.length > 0 ? s[s.length - 1] : "");
        templateReplacementRules.put(
                "ship",
                shipFunction);
        templateReplacementRules.put(
                "warship",
                templateReplacementRules.get("ship"));
        templateReplacementRules.put(
                "chem",
                s -> s.length > 1 ? String.join("", Arrays.copyOfRange(s, 1, s.length - 1)) : "");
        templateReplacementRules.put(
                "resize",
                s -> s.length == 3 ? s[2] : s[1]);
        templateReplacementRules.put(
                "replace",
                s -> s.length == 4 ? s[1].replace(s[2],s[3]) : s[1]);
        templateReplacementRules.put(
                "val",
                s -> s.length > 1? s[1]: "");
        templateReplacementRules.put(
                "stnlnk",
                s -> s.length == 3 ? s[1] + " railway station (" + s[2] + ")" : s[1] + " railway station");
        templateReplacementRules.put(
                "stnlink",
                templateReplacementRules.get("stnlnk"));
        templateReplacementRules.put(
                "rws",
                templateReplacementRules.get("stnlnk"));
        templateReplacementRules.put(
                "'",
                s -> "'");
        templateReplacementRules.put(
                "snd",
                s -> " - ");
        templateReplacementRules.put(
                "col-break",
                s -> " ");
        templateReplacementRules.put(
                "chem2",
                s -> s.length > 1? s[1]: "");
        templateReplacementRules.put(
                "ct",
                s -> s.length == 2 ? s[1] : s[1] + "(" + s[2] + ")");
        templateReplacementRules.put(
                "color",
                s -> s.length == 3? s[2] : "");
        templateReplacementRules.put(
                "colored text",
                templateReplacementRules.get("color"));
        templateReplacementRules.put(
                "colour",
                templateReplacementRules.get("color"));
        templateReplacementRules.put(
                "fgcolor",
                templateReplacementRules.get("color"));
        templateReplacementRules.put(
                "pi",
                s -> "pi");
        templateReplacementRules.put(
                "center",
                s -> s.length > 1? s[1] : "");
        templateReplacementRules.put(
                "centre",
                templateReplacementRules.get("center"));
        templateReplacementRules.put(
                "sclass",
                s -> s.length == 3 ? s[1] + " " + s[2] : "");
        templateReplacementRules.put(
                "sclass2",
                templateReplacementRules.get("sclass"));
        templateReplacementRules.put(
                "transl",
                s -> s.length == 3 ? s[2] : s[1]);
        templateReplacementRules.put(
                "tcg",
                s -> s.length > 1? s[1]: "");
        templateReplacementRules.put(
                "f1",
                s -> s.length > 1? s[1]: s[0]);
        templateReplacementRules.put(
                "\"'",
                s -> "'");
        templateReplacementRules.put(
                "nsmdns",
                templateReplacementRules.get("snd"));
        templateReplacementRules.put(
                "SMS",
                ussFunction);
        templateReplacementRules.put(
                "ms",
                s -> s.length > 1? s[1]: "");
        templateReplacementRules.put(
                "small",
                s -> s.length > 1? s[1]: "");
    }

    /**
     * Extract caption from image tag.
     *
     * @param imageTag Wikitext image tag
     * @return Caption text
     */
    @Nullable
    @Override
    public String extract(final String imageTag) {
        final List<String> fileTagComp = tokenize(imageTag);

        String caption = fileTagComp.get(fileTagComp.size() - 1);

        for (Pattern pattern : NON_CAPTION_PATTERN) {
            if (pattern.matcher(caption).matches()) {
                return null;
            }
        }

        caption = StringEscapeUtils.unescapeXml(caption);
        caption = StringEscapeUtils.unescapeHtml(caption);

        caption = REF_TAG_PATTERN.matcher(caption).replaceAll("");
        caption = XML_TAG_PATTERN.matcher(caption).replaceAll(" ");

        caption = cleanTemplates(caption);

        caption = CITATION_PATTERN.matcher(caption).replaceAll("");

        final String captionCopy = caption;
        final WikiTextImageExtractor imageExtractor = new WikiTextImageExtractor(null);
        imageExtractor.reset(captionCopy);

        while (imageExtractor.next()){
            final String imgTag = imageExtractor.getCurrent();
            final String subcaption = extract(imgTag);

            caption = caption.replace(imgTag, subcaption == null ? "": " " + subcaption + " ");
        }

        final Matcher wikiLinkMatcher = WIKILINK_PATTERN.matcher(caption);

        while (wikiLinkMatcher.find()) {
            final String link = wikiLinkMatcher.group(0);

            caption = caption.replace(link, link.substring(2, link.length() - 2));
        }

        final Matcher namedWikiLinkMatcher = NAMED_WIKILINK_PATTERN.matcher(caption);

        while (namedWikiLinkMatcher.find()) {
            final String match = namedWikiLinkMatcher.group(0);
            final String parts = match.substring(2, match.length() - 2);

            final String[] comp = parts.split("\\|");

            if (comp.length == 2) {
                caption = caption.replace(match, comp[1]);
            }
        }

        final Matcher externalLinkMatcher = EXTERNAL_LINK_PATTERN.matcher(caption);

        while (externalLinkMatcher.find()){
            final String match = externalLinkMatcher.group(0);
            final String[] matchComp = match.split("[ \\]]");
            final String linkText = String.join(" ", Arrays.copyOfRange(matchComp, 1 , matchComp.length));
            caption = caption.replace(match, linkText);
        }

        caption = BOLD_PATTERN.matcher(caption).replaceAll("");
        caption = ITALIC_PATTERN.matcher(caption).replaceAll("");

        if (caption.trim().isEmpty()) {
            return null;
        }

        return WHITESPACE_NORMALIZER.process(caption);
    }

    /**
     * Get the type of the caption this algorithm extracts.
     *
     * @return CaptionType.WIKI_TEXT
     */
    @Override
    public CaptionType getCaptionType() {
        return CaptionType.WIKI_TEXT;
    }

    public static List<String> tokenize(final String wikiText) {
        List<String> comp = new LinkedList<>();

        String wikiTextCopy = wikiText.substring(2, wikiText.length() - 2);
        int openBraces = 0;
        int openSquares = 0;
        int openAngleBrackes = 0;
        int lastSplitPos = 0;

        for (int i = 0; i < wikiTextCopy.length(); i++) {
            if (wikiTextCopy.charAt(i) == '|') {
                if (openSquares == 0 && openBraces == 0 && openAngleBrackes == 0) {
                    comp.add(wikiTextCopy.substring(lastSplitPos, i));
                    lastSplitPos = i + 1;
                }
            } else if (wikiTextCopy.charAt(i) == '[') {
                openSquares++;
            } else if (wikiTextCopy.charAt(i) == ']') {
                openSquares--;
            } else if (wikiTextCopy.charAt(i) == '{') {
                openBraces++;
            } else if (wikiTextCopy.charAt(i) == '}') {
                openBraces--;
            } else if (wikiTextCopy.charAt(i) == '<') {
                openAngleBrackes++;
            } else if (wikiTextCopy.charAt(i) == '>') {
                openAngleBrackes--;
            }
        }

        comp.add(wikiTextCopy.substring(lastSplitPos));

        return comp;
    }

    private String cleanTemplates(String caption) {
        bracketPosStack.clear();

        for (int i = 0; i < caption.length() - 1; i++) {
            char thisChar = caption.charAt(i);
            char nextChar = caption.charAt(i + 1);

            if (thisChar == '{' && nextChar == '{') {
                bracketPosStack.push(i);
                i++;
            } else if (thisChar == '}' && nextChar == '}') {
                if (!bracketPosStack.isEmpty()) {
                    int bracketStart = bracketPosStack.poll();
                    int bracketEnd = i + 2;

                    String template = caption.substring(bracketStart, bracketEnd);
                    String[] templateComp = tokenize(template).toArray(new String[0]);

                    if (templateComp.length > 0) {
                        String templateName = templateComp[0].toLowerCase().trim();

                        if (templateReplacementRules.containsKey(templateName)) {
                            String replacement = templateReplacementRules.get(templateName)
                                    .apply(templateComp);
                            int diff = template.length() + 4 - replacement.length();

                            caption = caption.substring(0, bracketStart)
                                    + replacement
                                    + caption.substring(bracketEnd);

                            i -= diff;
                            i = Math.max(i, 0);
                        }else{
                            caption = caption.substring(0, bracketStart)
                                    + caption.substring(bracketEnd);
                        }
                    }
                }

                i++;
            }
        }

        return caption;
    }
}


