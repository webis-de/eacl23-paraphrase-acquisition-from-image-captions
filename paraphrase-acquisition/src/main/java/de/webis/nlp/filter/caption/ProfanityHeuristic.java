package de.webis.nlp.filter.caption;

import de.webis.hadoop.counter.CaptionFilterCounter;
import de.webis.hadoop.counter.TableCounter;
import edu.stanford.nlp.ling.CoreLabel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ProfanityHeuristic implements CaptionFilterHeuristic {
    private static final Set<String> OFFENSIVE_WORDS = ConcurrentHashMap.newKeySet();

    static {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                        ProfanityHeuristic.class.getResourceAsStream("/luis-von-ahn/bad-words.txt")
                )
        );

        String line;
        try {
            while ((line = reader.readLine()) != null) {
                OFFENSIVE_WORDS.add(line);
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean accept(String text, List<CoreLabel> tokens) {
        for (CoreLabel token : tokens) {
            if (OFFENSIVE_WORDS.contains(token.value().toLowerCase())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public CaptionFilterCounter getCounterType() {
        return CaptionFilterCounter.PROFANITY;
    }

    @Override
    public TableCounter getPassCounterCaptions() {
        return null;
    }
}
