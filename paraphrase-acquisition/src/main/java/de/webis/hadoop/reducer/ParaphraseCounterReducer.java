package de.webis.hadoop.reducer;

import de.webis.caption_extraction.CaptionType;
import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParaphraseCounterReducer extends Reducer<BytesWritable, ImageReferenceWritable, NullWritable, NullWritable> {

    public ParaphraseCounterReducer() {
    }

    @Override
    protected void reduce(BytesWritable key, Iterable<ImageReferenceWritable> imageReferenceWritables, Context context) throws IOException, InterruptedException {
        context.getCounter(TableCounter.A_IMAGES_NO_FILTER).increment(1L);

        Map<CaptionType, Integer> referencesPerCaption = new HashMap<>();
        List<String> ids = new ArrayList<>();
        int references = 0;

        for (ImageReferenceWritable imageReferenceWritable : imageReferenceWritables) {
            for (CaptionType captionType : imageReferenceWritable.getCaptions().keySet()) {
                referencesPerCaption.putIfAbsent(captionType, 0);
                referencesPerCaption.put(captionType, referencesPerCaption.get(captionType) + 1);
            }

            references++;
            ids.add(String.valueOf(imageReferenceWritable.getID()));
        }

        if (references >= 2) {
            context.getCounter(TableCounter.A_REFERENCES_GEQ_2).increment(1L);

            if (references >= 5) {
                context.getCounter(TableCounter.A_REFERENCES_GEQ_5).increment(1L);
            }
        } else {
            if (context.getJobName().equals("paraphrase-acquisition-analysis-wikipedia-1")) {
                System.out.println(ids + " | " + new String(key.getBytes()));
                Thread.sleep(60000);
            }
        }

        for (Integer referencePerCaption : referencesPerCaption.values()) {
            int paraphrases = (referencePerCaption * (referencePerCaption - 1)) / 2;
            context.getCounter(TableCounter.A_PARAPHRASE_PAIRS_NO_FILTER).increment(paraphrases);
        }
    }
}
