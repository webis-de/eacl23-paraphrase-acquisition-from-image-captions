package webis.caption_extraction;

import de.webis.caption_extraction.CaptionExtractor;
import de.webis.caption_extraction.WikiAltTagExtractor;
import de.webis.caption_extraction.WikiTextExtractor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCaptionExtraction {
    @Test
    public void testWikiTextExtraction() throws IOException {
        CaptionExtractor wikiTextExtractor = new WikiTextExtractor();
        Reader reader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/caption-extraction-test.csv")));
        CSVParser parser = new CSVParser(
                reader, CSVFormat.DEFAULT.withSkipHeaderRecord());

        for (CSVRecord record : parser) {
            String expected = record.get(1);
            if (expected.equals("null")) {
                expected = null;
            }

            assertEquals(expected, wikiTextExtractor.extract(record.get(0)));
        }

        parser.close();
    }

    @Test
    public void testWikiTextAltTagExtraction() {
        CaptionExtractor altTagExtractor = new WikiAltTagExtractor();

        assertEquals("Painting of Napoleon Bonaparte in His Study at the Tuileries",
                altTagExtractor.extract("[[File:Jacques-Louis David 017.jpg |thumb |upright=0.75 |alt=Painting of Napoleon Bonaparte in His Study at the Tuileries |''[[The Emperor Napoleon in His Study at the Tuileries]]'' by [[Jacques-Louis David]]]]"));
    }
}
