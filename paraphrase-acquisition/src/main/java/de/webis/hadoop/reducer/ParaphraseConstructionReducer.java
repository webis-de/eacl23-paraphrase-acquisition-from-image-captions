package de.webis.hadoop.reducer;

import de.webis.caption_extraction.CaptionType;
import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Map;

public class ParaphraseConstructionReducer extends Reducer<BytesWritable, ImageReferenceWritable, IntWritable, ParaphraseWritable> {
    private final IntWritable paraphraseHash;
    private final ParaphraseWritable paraphraseWritable;

    private final DataOutputBuffer annotationsBufferFirst;
    private final DataOutputBuffer annotationsBufferSecond;

    private final DataInputBuffer annotationInBufferFirst;
    private final DataInputBuffer annotationsInBufferSecond;

    private MultipleOutputs<?, ?> multipleOutputs;
    private int startStage;

    public ParaphraseConstructionReducer() {
        paraphraseHash = new IntWritable();
        paraphraseWritable = new ParaphraseWritable();
        annotationsBufferFirst = new DataOutputBuffer();
        annotationsBufferSecond = new DataOutputBuffer();

        annotationInBufferFirst = new DataInputBuffer();
        annotationsInBufferSecond = new DataInputBuffer();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
        startStage = context.getConfiguration().getInt("filter.stage.start", -1);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        annotationsBufferFirst.close();
        annotationsBufferSecond.close();

        annotationInBufferFirst.close();
        annotationsInBufferSecond.close();

        multipleOutputs.close();
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<ImageReferenceWritable> imageReferenceWritables, Context context) throws IOException, InterruptedException {
        org.apache.hadoop.mapreduce.Counter counter = context.getCounter(Counter.CLUSTER_ID);
        context.getCounter(TableCounter.A_IMAGES_NO_FILTER).increment(1L);

        long clusterId = counter.getValue();
        counter.increment(1L);

        int bufferSize = 0;

        annotationsBufferFirst.reset();
        annotationsBufferSecond.reset();

        ImageReferenceWritable first = new ImageReferenceWritable();
        ImageReferenceWritable second = new ImageReferenceWritable();

        for (ImageReferenceWritable imageReferenceWritable : imageReferenceWritables) {
            imageReferenceWritable.write(annotationsBufferFirst);
            imageReferenceWritable.write(annotationsBufferSecond);

            bufferSize++;

            if (bufferSize >= 2) {
                if (bufferSize == 2) {
                    annotationInBufferFirst.reset(annotationsBufferFirst.getData(), 0, annotationsBufferFirst.getLength());
                    first.readFields(annotationInBufferFirst);

                    multipleOutputs.write(String.valueOf(startStage), first, NullWritable.get(), startStage + "/references");
                }

                multipleOutputs.write(String.valueOf(startStage), imageReferenceWritable, NullWritable.get(), startStage + "/references");
            }
        }

        if (bufferSize >= 2) {
            context.getCounter(TableCounter.A_REFERENCES_GEQ_2).increment(1L);

            if (bufferSize >= 5) {
                context.getCounter(TableCounter.A_REFERENCES_GEQ_5).increment(1L);
            }
        }

        annotationInBufferFirst.reset(annotationsBufferFirst.getData(), 0, annotationsBufferFirst.getLength());

        for (int i = 0; i < bufferSize; i++) {
            first.readFields(annotationInBufferFirst);

            annotationsInBufferSecond.reset(annotationsBufferSecond.getData(), 0, annotationsBufferSecond.getLength());
            for (int j = 0; j < bufferSize; j++) {
                second.readFields(annotationsInBufferSecond);

                if (j <= i) {
                    continue;
                }

                for (Map.Entry<CaptionType, String> firstCaption : first.getCaptions().entrySet()) {
                    if (second.getCaptions().containsKey(firstCaption.getKey())) {
                        paraphraseWritable.set(
                                clusterId,
                                firstCaption.getValue(), first.getPageUri(), first.getImageUri(), firstCaption.getKey().name(),
                                second.getCaptions().get(firstCaption.getKey()), second.getPageUri(), second.getImageUri(), firstCaption.getKey().name());

                        paraphraseHash.set(paraphraseWritable.hashCode());
                        context.write(paraphraseHash, paraphraseWritable);
                    }
                }
            }
        }
    }

    private enum Counter {
        CLUSTER_ID
    }
}
