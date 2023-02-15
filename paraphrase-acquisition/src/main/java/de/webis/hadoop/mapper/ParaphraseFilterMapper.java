package de.webis.hadoop.mapper;

import de.webis.caption_extraction.CaptionType;
import de.webis.hadoop.counter.ParaphraseFilterCounter;
import de.webis.hadoop.counter.PipelineCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import de.webis.nlp.filter.paraphrase.ParaphraseFilterHeuristic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class ParaphraseFilterMapper extends Mapper<IntWritable, ParaphraseWritable, IntWritable, ParaphraseWritable> {
    private final Set<Integer> paraphraseHashes;
    private final Set<String> referenceHashes;

    private final List<ParaphraseFilterHeuristic> paraphraseFilters;

    private int startStage;
    private MultipleOutputs<?, ?> multipleOutputs;

    private final ImageReferenceWritable imageReferenceWritable;

    public ParaphraseFilterMapper() {
        paraphraseHashes = new HashSet<>();
        referenceHashes = new HashSet<>();
        paraphraseFilters = new LinkedList<>();

        imageReferenceWritable = new ImageReferenceWritable();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();

        int numFilter = configuration.getInt("filter.paraphrases.count", -1);

        if (numFilter == -1) {
            throw new IOException("Can't find \"filter.paraphrases.count\" option!");
        }

        for (int i = 0; i < numFilter; i++) {
            Class<?> retrievedClass = configuration.getClass("filter.paraphrases." + i, null);

            if (retrievedClass == null) {
                throw new IOException("Unknown class for option \"filter.paraphrases." + i + "\"");
            }

            Class<? extends ParaphraseFilterHeuristic> filterClass = (Class<? extends ParaphraseFilterHeuristic>) retrievedClass;

            try {
                paraphraseFilters.add(filterClass.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IOException("Can't instantiate filter class \"" + filterClass.getCanonicalName() + "\"");
            }
        }

        startStage = configuration.getInt("filter.stage.start", -1);
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(IntWritable key, ParaphraseWritable paraphrase, Context context) throws IOException, InterruptedException {

        context.getCounter(PipelineCounter.INPUT_PARAPHRASE_FILTER).increment(1L);

        if (paraphraseHashes.contains(paraphrase.hashCode())) {
            context.getCounter(ParaphraseFilterCounter.DUPLICATE).increment(1L);
            return;
        } else {
            addReferences(paraphrase, startStage);
        }
        paraphraseHashes.add(paraphrase.hashCode());

        boolean accept = true;

        int currentStage = startStage + 1;
        for (ParaphraseFilterHeuristic filterHeuristic : paraphraseFilters) {
            if (!filterHeuristic.accept(paraphrase)) {
                accept = false;

                context.getCounter(filterHeuristic.getCounterType()).increment(1L);
                break;
            } else {
                addReferences(paraphrase, currentStage);

                currentStage++;
            }
        }

        if (accept) {
            context.write(key, paraphrase);
        }
    }

    private void addReferences(ParaphraseWritable paraphrase, int currentStage) throws IOException, InterruptedException {
//        System.out.println(currentStage + ": " + paraphrase.toString());
        imageReferenceWritable.clear();

        imageReferenceWritable.setImageUri(paraphrase.getFirstImageUri());
        imageReferenceWritable.setPageUri(paraphrase.getFirstPageUri());
        imageReferenceWritable.addCaption(CaptionType.valueOf(paraphrase.getFirstCaptionType()), paraphrase.getFirst());

        if (!referenceHashes.contains(currentStage + "-" + imageReferenceWritable.hashCode())) {
            multipleOutputs.write(String.valueOf(currentStage), imageReferenceWritable, NullWritable.get(), currentStage + "/references");
        }

        referenceHashes.add(currentStage + "-" + imageReferenceWritable.hashCode());

        imageReferenceWritable.clear();

        imageReferenceWritable.setImageUri(paraphrase.getSecondImageUri());
        imageReferenceWritable.setPageUri(paraphrase.getSecondPageUri());
        imageReferenceWritable.addCaption(CaptionType.valueOf(paraphrase.getSecondCaptionType()), paraphrase.getSecond());

        if (!referenceHashes.contains(currentStage + "-" + imageReferenceWritable.hashCode())) {
            multipleOutputs.write(String.valueOf(currentStage), imageReferenceWritable, NullWritable.get(), currentStage + "/references");
        }

        referenceHashes.add(currentStage + "-" + imageReferenceWritable.hashCode());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
