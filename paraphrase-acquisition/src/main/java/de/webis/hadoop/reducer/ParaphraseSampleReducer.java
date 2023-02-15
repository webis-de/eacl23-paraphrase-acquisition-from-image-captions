package de.webis.hadoop.reducer;

import de.webis.hadoop.formats.writables.ParaphraseWritable;
import de.webis.hadoop.formats.writables.SecondarySortWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ParaphraseSampleReducer extends Reducer<SecondarySortWritable, ParaphraseWritable, LongWritable, ParaphraseWritable> {
    private int numSamples;

    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();

        numSamples = Integer.parseInt(configuration.get("num-samples"));
    }

    @Override
    protected void reduce(SecondarySortWritable key, Iterable<ParaphraseWritable> captions, Context context) throws IOException, InterruptedException {
        long count = 0L;

        for (ParaphraseWritable paraphraseWritable : captions) {
            if (count == numSamples) {
                break;
            }


            context.write(new LongWritable(count), paraphraseWritable);
            count++;
        }
    }
}
