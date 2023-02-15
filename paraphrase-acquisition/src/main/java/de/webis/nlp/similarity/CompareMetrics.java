package de.webis.nlp.similarity;

import de.webis.hadoop.formats.writables.ParaphraseWritable;
import org.apache.commons.csv.*;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class CompareMetrics {

    public static List<CSVRecord> loadGCCParaphrases(String path) throws IOException {
        CSVParser parser = CSVParser.parse(new BufferedReader(new FileReader(path)), CSVFormat.DEFAULT);

        return parser.getRecords();
    }

    public static void main(String[] args) {
        List<ParaphraseSimilarity> similarityMetrics = new LinkedList<>(Arrays.asList(
                new LevenshteinDistance(),
                new WordNGramOverlap(),
                new LCPNGramOverlap(),
                new BleuMetric(),
                new SumoMetric())
        );

//        List<ParaphraseWritable> testParaphrases = new LinkedList<>(Arrays.asList(
//                new ParaphraseWritable("These are similar texts.", "these are close texts."),
//                new ParaphraseWritable("This statement is true", "This statement is false"),
//                new ParaphraseWritable("An American child purchases a can of V8, handing the grocer his ration book.", "An American child during World War II purchases a can of V8, handing the grocer his ration book."),
//                new ParaphraseWritable("This text has nothing to do with the other", "No word is shared."),
//                new ParaphraseWritable("on july troy is expected to be sentenced to life in prison without parole", "troy faces life in prison without parole at his july sentencing")
//        ));

        try {
            List<CSVRecord> records = loadGCCParaphrases("./data/conceptual-caption-paraphrases/gcc_paraphrases_annotations.csv");
            CSVPrinter printer = new CSVPrinter(new BufferedWriter(new FileWriter("./data/conceptual-caption-paraphrases/gcc_paraphrases_annotations_assessed.csv")),
                    CSVFormat.DEFAULT.withQuoteMode(QuoteMode.NON_NUMERIC));

            for (CSVRecord record : records) {
                ParaphraseWritable paraphraseWritable = new ParaphraseWritable(record.get(0), record.get(1));

                System.out.printf(
                        "-------------------------\n" +
                                "%20s: \"%s\"\n" +
                                "%20s: \"%s\"\n" +
                                "-------------------------\n",
                        "First", paraphraseWritable.getFirst(),
                        "Second", paraphraseWritable.getSecond());

                for (int i = 0; i < record.size(); i++) {
                    if (i == 3 || i == 4) {
                        printer.print("1".equals(record.get(i)));
                    } else {
                        printer.print(record.get(i));
                    }
                }

                for (ParaphraseSimilarity similarity : similarityMetrics) {
                    double score = similarity.score(paraphraseWritable.getFirst(), paraphraseWritable.getSecond());
                    System.out.printf("%20s: %1.3f\n", similarity.getClass().getSimpleName(), score);
                    printer.print(score);
                }
                printer.println();
                System.out.println("-------------------------\n");
            }

            printer.flush();
            printer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
