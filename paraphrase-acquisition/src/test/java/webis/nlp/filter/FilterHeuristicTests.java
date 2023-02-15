package webis.nlp.filter;


import de.webis.nlp.filter.caption.CaptionFilterHeuristic;
import de.webis.nlp.filter.caption.LanguageHeuristic;
import de.webis.nlp.filter.caption.TokenCountHeuristic;
import de.webis.nlp.filter.paraphrase.CommonWordHeuristic;
import de.webis.nlp.filter.paraphrase.ParaphraseFilterHeuristic;
import de.webis.nlp.tokenizer.StanfordWordTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FilterHeuristicTests {
    private final Tokenizer tokenizer;

    public FilterHeuristicTests() {
        tokenizer = new StanfordWordTokenizer();
    }

    @Test
    public void testTokenCountHeuristic() {
        CaptionFilterHeuristic heuristic = new TokenCountHeuristic();

        String text = "test";
        List<CoreLabel> tokens = tokenizer.tokenize(text);

        assertFalse(heuristic.accept(text, tokens), "Single-token texts should not be accepted!");

        text = "this is a test with more than 6 tokens!";
        tokens = tokenizer.tokenize(text);

        assertTrue(heuristic.accept(text, tokens), "Multi-token texts should be accepted if they have more than 10!");
    }

    @Test
    public void testCommonWordHeuristic() {
        ParaphraseFilterHeuristic heuristic = new CommonWordHeuristic();

        String text1 = "this is an arbitrary sentence!";
        String text2 = "this is also some sentence";

        List<CoreLabel> tokens1 = tokenizer.tokenize(text1);
        List<CoreLabel> tokens2 = tokenizer.tokenize(text2);

        assertTrue(heuristic.accept(text1, tokens1, text2, tokens2),
                "Pairs with common words should be accepted!");

        text1 = "we have here a sentence with special characters, like ! or :)";
        text2 = "something completely different with ! and :)";

        tokens1 = tokenizer.tokenize(text1);
        tokens2 = tokenizer.tokenize(text2);

        assertFalse(heuristic.accept(text1, tokens1, text2, tokens2),
                "Pairs with only common character should not be accepted!");

    }
}
