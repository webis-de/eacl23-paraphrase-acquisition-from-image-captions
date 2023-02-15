package de.webis.hadoop.mapper;

import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ReferenceInputMapper extends Mapper<ImageReferenceWritable, NullWritable, ImageReferenceWritable, NullWritable> {

    @Override
    protected void map(ImageReferenceWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.getCounter(TableCounter.A_REFERENCES_NO_FILTER).increment(1L);
        context.getCounter(TableCounter.A_CAPTIONS_NO_FILTER).increment(key.getCaptions().size());
        context.write(key, value);
    }
}
