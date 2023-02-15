package de.webis.parser.uri;

import io.mola.galimatias.GalimatiasParseException;
import io.mola.galimatias.URL;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class URIParser {
    private final static Pattern QUOTE_PATTERN = Pattern.compile("^\\\\\"|^\"|^'|\\\\\"$|\"$|'$");

    public static URI parse(URI baseURI, String path) {
        URI parsedURI;

        path = QUOTE_PATTERN.matcher(path).replaceAll("");
        try {
            parsedURI = URL.parse(URL.fromJavaURI(baseURI), path).toJavaURI();
        } catch (GalimatiasParseException | URISyntaxException | RuntimeException e) {
            parsedURI = null;
        }

        return parsedURI;
    }
}
