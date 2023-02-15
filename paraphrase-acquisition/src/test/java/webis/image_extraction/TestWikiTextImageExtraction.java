package webis.image_extraction;

import de.webis.parser.wikitext.WikiTextImageExtractor;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestWikiTextImageExtraction {
    private final WikiTextImageExtractor wikiTextImageExtractor;


    public TestWikiTextImageExtraction() {
        wikiTextImageExtractor = new WikiTextImageExtractor(null);
    }

    @Test
    public void testWarshipMonitor() throws IOException {
        testFiles(
                TestWikiTextImageExtraction.class.getResourceAsStream("/monitor-warship.txt"),
                TestWikiTextImageExtraction.class.getResourceAsStream("/monitor-warship-images.txt"));
    }

    @Test
    public void test2Cellos() throws IOException {
        testFiles(
                TestWikiTextImageExtraction.class.getResourceAsStream("/2cellos.txt"),
                TestWikiTextImageExtraction.class.getResourceAsStream("/2cellos-images.txt"));
    }

    private void testFiles(InputStream wikiTextInputStream, InputStream imageInputStream) throws IOException {
        String line;

        BufferedReader reader = new BufferedReader(new InputStreamReader(wikiTextInputStream));
        StringBuilder wikitextBuilder = new StringBuilder();

        while ((line = reader.readLine()) != null) {
            wikitextBuilder.append(line);
        }

        reader.close();
        reader = new BufferedReader(new InputStreamReader(imageInputStream));

        Queue<String> images = new LinkedBlockingQueue<>();

        while ((line = reader.readLine()) != null) {
            images.add(line);
        }

        reader.close();

        test(wikitextBuilder.toString(), images);
    }

    private void test(String wikitext, Queue<String> images) {
        wikiTextImageExtractor.reset(wikitext);

        while (wikiTextImageExtractor.next()) {
            assertEquals(images.poll(), wikiTextImageExtractor.getCurrent());
        }
    }
}
