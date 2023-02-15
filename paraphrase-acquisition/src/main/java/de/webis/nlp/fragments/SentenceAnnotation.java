package de.webis.nlp.fragments;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SentenceAnnotation {
    private final int id;
    private final String text;
    private final String mainClause;

    private boolean isSentence;

    public SentenceAnnotation(int id, String text, boolean isSentence) {
        this.id = id;
        this.text = text;
        this.isSentence = isSentence;
        mainClause = null;
    }

    public SentenceAnnotation(int id, String text, String mainClause, boolean isSentence) {
        this.id = id;
        this.text = text;
        this.mainClause = mainClause;
        this.isSentence = isSentence;
    }

    public static List<SentenceAnnotation> loadAnnotations(String path) throws IOException {
        List<SentenceAnnotation> annotations = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(path));

        CSVParser parser = CSVParser.parse(reader, CSVFormat.DEFAULT.withAllowMissingColumnNames());

        for (CSVRecord record : parser) {
            boolean isSentence = Boolean.parseBoolean(record.get(2));
            if (isSentence)
                annotations.add(new SentenceAnnotation(
                        Integer.parseInt(record.get(0)),
                        record.get(1),
                        record.get(3),
                        true
                ));
            else {
                annotations.add(new SentenceAnnotation(
                        Integer.parseInt(record.get(0)),
                        record.get(1),
                        false
                ));
            }

        }

        return annotations;
    }

    public int getId() {
        return id;
    }

    public String getText() {
        return text;
    }

    public boolean isSentence() {
        return isSentence;
    }

    public SentenceAnnotation setSentence(boolean sentence) {
        isSentence = sentence;

        return this;
    }

    public String getMainClause() {
        return mainClause;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        return hashCode() == o.hashCode();
    }

    @Override
    public String toString() {
        return "SentenceAnnotation{" +
                "id=" + id +
                ", text='" + text + '\'' +
                ", isSentence=" + isSentence +
                '}';
    }
}
