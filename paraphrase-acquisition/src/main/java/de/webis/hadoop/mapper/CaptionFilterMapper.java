package de.webis.hadoop.mapper;

import de.webis.caption_extraction.CaptionType;
import de.webis.hadoop.counter.PipelineCounter;
import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.nlp.filter.caption.CaptionFilterHeuristic;
import de.webis.nlp.preprocessor.Preprocessor;
import de.webis.nlp.tokenizer.StanfordWordTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.*;

public class CaptionFilterMapper extends Mapper<ImageReferenceWritable, NullWritable, ImageReferenceWritable, NullWritable> {
    private int startStage;
    private final List<Preprocessor> captionPreprocessors;

    private final Tokenizer tokenizer;

    private final List<CaptionFilterHeuristic> captionFilters;
    private MultipleOutputs<?, ?> multipleOutputs;

    public CaptionFilterMapper() {
        captionPreprocessors = new LinkedList<>();
        captionFilters = new LinkedList<>();

        tokenizer = new StanfordWordTokenizer();
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();

        int numPreprocessor = configuration.getInt("filter.captions.pre.count", -1);

        if (numPreprocessor == -1) {
            throw new IOException("Can't find \"filter.captions.pre.count\" option!");
        }

        for (int i = 0; i < numPreprocessor; i++) {
            Class<?> retrievedClass = configuration.getClass("filter.captions.pre." + i, null);

            if (retrievedClass == null) {
                throw new IOException("Unknown class for option \"filter.captions.pre." + i + "\"");
            }

            Class<? extends Preprocessor> preprocessorClass =
                    (Class<? extends Preprocessor>) retrievedClass;

            try {
                captionPreprocessors.add(preprocessorClass.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("Can't instantiate preprocessor class \"" + preprocessorClass.getCanonicalName() + "\"");
            }
        }

        startStage = configuration.getInt("filter.stage.start", -1);
        int numCaptionFilter = configuration.getInt("filter.captions.count", -1);

        if (numCaptionFilter == -1) {
            throw new IOException("Can't find \"filter.captions.count\" option!");
        }

        for (int i = 0; i < numCaptionFilter; i++) {
            Class<?> retrievedClass = configuration.getClass("filter.captions." + i, null);

            if (retrievedClass == null) {
                throw new IOException("Unknown class for option \"filter.captions." + i + "\"");
            }

            Class<? extends CaptionFilterHeuristic> captionFilterClass =
                    (Class<? extends CaptionFilterHeuristic>) retrievedClass;

            try {
                captionFilters.add(captionFilterClass.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("Can't instantiate filter class \"" + captionFilterClass.getCanonicalName() + "\"");
            }
        }

        multipleOutputs = new MultipleOutputs<>(context);

        /*defaultPassesDocument.put("cf_HasCaption", 0);
        defaultPassesDocument.put("cf_HasPassingCaption", 0);

        String host = configuration.get("mongodb.host");
        int port = configuration.getInt("mongodb.port", -1);
        String database = configuration.get("mongodb.database");

        mongoClient = new MongoClient(host, port);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        imageCollection = mongoDatabase.getCollection("images");*/
    }

    @Override
    protected void map(ImageReferenceWritable imageReferenceWritable, NullWritable nullWritable, Context context) throws IOException, InterruptedException {
        context.getCounter(PipelineCounter.INPUT_CAPTION_FILTER_REFERENCES).increment(1L);
        context.getCounter(PipelineCounter.INPUT_CAPTION_FILTER_CAPTIONS).increment(imageReferenceWritable.getCaptions().size());

        if (!imageReferenceWritable.getCaptions().isEmpty()) {
            context.getCounter(TableCounter.C_REFERENCES_HAVE_CAPTIONS).increment(1L);
            context.getCounter(TableCounter.C_CAPTIONS_HAVE_CAPTIONS).increment(imageReferenceWritable.getCaptions().size());
        } else {
            return;
        }

        Iterator<Map.Entry<CaptionType, String>> captionIter =
                imageReferenceWritable.getCaptions().entrySet().iterator();

        Map<CaptionType, List<CoreLabel>> tokens = new HashMap<>();

        while (captionIter.hasNext()) {
            Map.Entry<CaptionType, String> captionEntry = captionIter.next();
            String processedCaption = captionEntry.getValue();

            for (Preprocessor processor : captionPreprocessors) {
                processedCaption = processor.process(processedCaption);
            }

            imageReferenceWritable.addCaption(captionEntry.getKey(), processedCaption);
            tokens.put(captionEntry.getKey(), tokenizer.tokenize(processedCaption));
        }

        int currentStage = startStage;
        for (CaptionFilterHeuristic captionFilterHeuristic : captionFilters) {
            captionIter = imageReferenceWritable.getCaptions().entrySet().iterator();
            while (captionIter.hasNext()) {
                Map.Entry<CaptionType, String> captionEntry = captionIter.next();

                if (!captionFilterHeuristic.accept(captionEntry.getValue(), tokens.get(captionEntry.getKey()))) {
                    captionIter.remove();
                }
            }

            if (!imageReferenceWritable.getCaptions().isEmpty()) {
                multipleOutputs.write(String.valueOf(currentStage), imageReferenceWritable, nullWritable, currentStage + "/references");
                currentStage++;
            }
        }

        if (!imageReferenceWritable.getCaptions().isEmpty()) {
            context.write(imageReferenceWritable, nullWritable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
