package de.webis.hadoop.formats.writables;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import kotlin.NotImplementedError;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.*;

public class ParaphraseWritable implements Writable, JsonSerializable {
    private long clusterId;

    private String first;
    private URI firstPageUri;
    private URI firstImageUri;
    private String firstCaptionType;

    private String second;
    private URI secondPageUri;
    private URI secondImageUri;
    private String secondCaptionType;

    private final Map<String, Double> similarities;

    public ParaphraseWritable() {
        first = null;
        firstPageUri = null;
        firstImageUri = null;
        firstCaptionType = null;

        second = null;
        secondPageUri = null;
        secondImageUri = null;
        secondCaptionType = null;

        similarities = new LinkedHashMap<>();
    }

    public ParaphraseWritable(String first, String second) {
        this.first = first;
        this.second = second;

        sort();
        similarities = new LinkedHashMap<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(clusterId);

        dataOutput.writeUTF(first);
        dataOutput.writeUTF(firstPageUri.toString());
        dataOutput.writeUTF(firstImageUri.toString());
        dataOutput.writeUTF(firstCaptionType);

        dataOutput.writeUTF(second);
        dataOutput.writeUTF(secondPageUri.toString());
        dataOutput.writeUTF(secondImageUri.toString());
        dataOutput.writeUTF(secondCaptionType);

        dataOutput.writeInt(similarities.size());

        for (Map.Entry<String, Double> similarity : similarities.entrySet()) {
            dataOutput.writeUTF(similarity.getKey());
            dataOutput.writeDouble(similarity.getValue());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        clusterId = dataInput.readLong();

        first = dataInput.readUTF();
        firstPageUri = URI.create(dataInput.readUTF());
        firstImageUri = URI.create(dataInput.readUTF());
        firstCaptionType = dataInput.readUTF();

        second = dataInput.readUTF();
        secondPageUri = URI.create(dataInput.readUTF());
        secondImageUri = URI.create(dataInput.readUTF());
        secondCaptionType = dataInput.readUTF();

        similarities.clear();

        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            String key = dataInput.readUTF();
            similarities.put(key, dataInput.readDouble());
        }

    }

    @Override
    public String toString() {
        List<Object> csvEntry = new LinkedList<>(Arrays.asList(
                hashCode(),
                clusterId,
                first,
                firstPageUri.toString(),
                firstImageUri.toString(),
                firstCaptionType,
                second,
                secondPageUri.toString(),
                secondImageUri.toString(),
                secondCaptionType
        ));

        for (Map.Entry<String, Double> entry : similarities.entrySet()) {
            csvEntry.add(String.format("%1.4f", entry.getValue()));
        }

        StringWriter out = new StringWriter();
        try (CSVPrinter printer = new CSVPrinter(out, CSVFormat.RFC4180
                .withQuoteMode(QuoteMode.NON_NUMERIC).withEscape('"'))) {
            printer.printRecord(csvEntry);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return out.toString().replace("^M", "").replace("\n", "");
    }

    @Override
    public int hashCode() {
        return (first + second).toLowerCase().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ParaphraseWritable)
            return hashCode() == o.hashCode();

        return false;
    }

    public String getFirst() {
        return first;
    }

    public String getSecond() {
        return second;
    }

    public void set(long clusterId,
                    String first, URI firstPageUri, URI firstImageUri, String firstCaptionType,
                    String second, URI secondPageUri, URI secondImageUri, String secondCaptionType) {

        this.clusterId = clusterId;
        this.first = first;
        this.firstPageUri = firstPageUri;
        this.firstImageUri = firstImageUri;
        this.firstCaptionType = firstCaptionType;

        this.second = second;
        this.secondPageUri = secondPageUri;
        this.secondImageUri = secondImageUri;
        this.secondCaptionType = secondCaptionType;

        sort();
    }

    public void addSimilarity(String name, Double value) {
        similarities.put(name, value);
    }

    private void sort() {
        if (first == null || second == null) {
            return;
        }

        if (first.compareToIgnoreCase(second) < 0) {
            String temp = first;
            first = second;
            second = temp;

            URI tempUri = firstPageUri;
            firstPageUri = secondPageUri;
            secondPageUri = tempUri;

            tempUri = firstImageUri;
            firstImageUri = secondImageUri;
            secondImageUri = tempUri;

            temp = firstCaptionType;
            firstCaptionType = secondCaptionType;
            secondCaptionType = temp;
        }
    }

    @Override
    public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeNumberField("id", hashCode());
        jsonGenerator.writeNumberField("cluster-id", clusterId);
        jsonGenerator.writeStringField("image", firstImageUri.toString());

        jsonGenerator.writeFieldName("first");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("caption-text", first);
        jsonGenerator.writeStringField("caption-type", firstCaptionType);
        jsonGenerator.writeStringField("page", firstPageUri.toString());
        jsonGenerator.writeEndObject();

        jsonGenerator.writeFieldName("second");
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("caption-text", second);
        jsonGenerator.writeStringField("caption-type", secondCaptionType);
        jsonGenerator.writeStringField("page", secondPageUri.toString());
        jsonGenerator.writeEndObject();

        jsonGenerator.writeFieldName("similarities");
        jsonGenerator.writeStartObject();
        for (Map.Entry<String, Double> similarity : similarities.entrySet()) {
            jsonGenerator.writeNumberField(similarity.getKey(), similarity.getValue());
        }
        jsonGenerator.writeEndObject();

        jsonGenerator.writeEndObject();
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
        throw new NotImplementedError();
    }

    public long getClusterId() {
        return clusterId;
    }

    public URI getFirstPageUri() {
        return firstPageUri;
    }

    public URI getFirstImageUri() {
        return firstImageUri;
    }

    public String getFirstCaptionType() {
        return firstCaptionType;
    }

    public URI getSecondPageUri() {
        return secondPageUri;
    }

    public URI getSecondImageUri() {
        return secondImageUri;
    }

    public String getSecondCaptionType() {
        return secondCaptionType;
    }

    public Map<String, Double> getSimilarities() {
        return similarities;
    }
}
