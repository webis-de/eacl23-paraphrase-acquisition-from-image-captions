package de.webis.hadoop.reducer;

import de.webis.hadoop.formats.writables.SecondarySortWritable;
import de.webis.nlp.tokenizer.StanfordWordTokenizer;
import de.webis.nlp.tokenizer.Tokenizer;
import edu.stanford.nlp.ling.CoreLabel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;

public class CaptionSampleReducer extends Reducer<SecondarySortWritable, Text, LongWritable, Text> {
    private int numSamples;

    private Tokenizer tokenizer;

    @Override
    protected void setup(Context context) {
        Configuration configuration = context.getConfiguration();

        numSamples = Integer.parseInt(configuration.get("num-samples"));
        tokenizer = new StanfordWordTokenizer();
    }

    @Override
    protected void reduce(SecondarySortWritable key, Iterable<Text> captions, Context context) throws IOException, InterruptedException {
        long count = 0L;

        for (Text caption : captions) {
            if (count == numSamples) {
                break;
            }

            List<CoreLabel> tokens = tokenizer.tokenize(caption.toString());

            if (tokens.size() >= 10) {
                context.write(new LongWritable(count), caption);
                count++;
            }
        }
    }
}
