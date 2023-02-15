package de.webis.nlp.fragments;

import de.webis.nlp.filter.caption.CaptionFilterHeuristic;
import de.webis.nlp.filter.caption.SentenceHeuristic;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class EvaluateSentenceHeuristic {
    public static void main(String[] args) {
        List<SentenceAnnotation> testAnnotations = null;

        try {
            testAnnotations
                    = SentenceAnnotation.loadAnnotations("data/wikipedia/sentence-detection/caption-sample-500-annotations.csv");
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        CaptionFilterHeuristic filterHeuristic = new SentenceHeuristic();

        Set<SentenceAnnotation> classifiedAnnotations = new HashSet<>(testAnnotations);
        classifiedAnnotations = classifiedAnnotations.stream().map(a -> new SentenceAnnotation(a.getId(), a.getText(), a.getMainClause(), filterHeuristic.accept(a.getText(), null))).collect(Collectors.toSet());

        Set<SentenceAnnotation> classifiedTrue = classifiedAnnotations.stream().filter(SentenceAnnotation::isSentence).collect(Collectors.toSet());
        Set<SentenceAnnotation> truePositives = testAnnotations.stream().filter(SentenceAnnotation::isSentence).collect(Collectors.toSet());
        truePositives.retainAll(classifiedTrue);

        Set<SentenceAnnotation> trueNegatives = testAnnotations.stream().filter(a -> !a.isSentence()).collect(Collectors.toSet());
        trueNegatives.retainAll(classifiedAnnotations.stream().filter(a -> !a.isSentence()).collect(Collectors.toSet()));

        Set<SentenceAnnotation> falsePositives = testAnnotations.stream().filter(a -> !a.isSentence()).collect(Collectors.toSet());
        falsePositives.retainAll(classifiedAnnotations.stream().filter(SentenceAnnotation::isSentence).collect(Collectors.toSet()));

        Set<SentenceAnnotation> falseNegatives = testAnnotations.stream().filter(SentenceAnnotation::isSentence).collect(Collectors.toSet());
        falseNegatives.retainAll(classifiedAnnotations.stream().filter(a -> !a.isSentence()).collect(Collectors.toSet()));

        double precision = (double) truePositives.size() / (truePositives.size() + falsePositives.size());
        double recall = (double) truePositives.size() / (truePositives.size() + falseNegatives.size());

        System.out.println(truePositives);
        System.out.println(falsePositives);

        System.out.printf("%10s: %1.2f\n%10s: %1.2f", "Precision", precision, "Recall", recall);
    }
}
