package de.webis.hadoop.mapper;

import de.webis.hadoop.formats.writables.ParaphraseWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class ParaphraseOutputMapper extends Mapper<IntWritable, ParaphraseWritable, IntWritable, ParaphraseWritable> {
    private MultipleOutputs<?, ?> multiOut;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multiOut = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(IntWritable key, ParaphraseWritable paraphrase, Context context) throws IOException, InterruptedException {
        multiOut.write("paraphrases", paraphrase, NullWritable.get());
        context.write(key, paraphrase);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multiOut.close();
    }
}
