package de.webis.hadoop.mapper;

import de.webis.hadoop.formats.writables.ParaphraseWritable;
import de.webis.hadoop.formats.writables.SecondarySortWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;

public class ParaphraseSamplingMapper extends Mapper<IntWritable, ParaphraseWritable, SecondarySortWritable, ParaphraseWritable> {
    private final Random random;
    private final String randKey;

    public ParaphraseSamplingMapper() {
        random = new Random(System.currentTimeMillis());
        randKey = "RAND";
    }

    @Override
    protected void map(IntWritable key, ParaphraseWritable paraphrase, Context context) throws IOException, InterruptedException {
        byte[] randBytes = new byte[32];
        random.nextBytes(randBytes);

        context.write(new SecondarySortWritable(randKey, new String(randBytes, StandardCharsets.UTF_8)), paraphrase);
    }
}
