package de.webis.hadoop.mapper;

import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.SecondarySortWritable;
import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

public class CaptionSamplingMapper extends Mapper<ImageReferenceWritable, NullWritable, SecondarySortWritable, Text> {
    private Random random;

    @Override
    protected void setup(Context context) {
        random = new Random(System.currentTimeMillis());
    }

    @Override
    protected void map(ImageReferenceWritable imageReferenceWritable, NullWritable nullWritable, Context context) throws IOException, InterruptedException {
        for (String caption : imageReferenceWritable.getCaptions().values()) {
            Reader reader = new StringReader(caption);
            DocumentPreprocessor preprocessor = new DocumentPreprocessor(reader);

            for (List<HasWord> sentence : preprocessor) {
                byte[] randBytes = new byte[32];
                random.nextBytes(randBytes);

                context.write(
                        new SecondarySortWritable("RAND", new String(randBytes, StandardCharsets.UTF_8)),
                        new Text(SentenceUtils.listToString(sentence)));
            }
        }
    }
}
