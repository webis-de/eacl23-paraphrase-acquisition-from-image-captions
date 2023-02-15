package de.webis.hadoop.formats.output;

import de.webis.hadoop.formats.writables.ImageWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MongoDBOutputFormat extends FileOutputFormat<ImageWritable, NullWritable> {
    private final MongoDBCommitter committer;

    public MongoDBOutputFormat() {
        committer = new MongoDBCommitter();
    }

    @Override
    public RecordWriter<ImageWritable, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MongoDBWriter(committer.getTaskPath(taskAttemptContext, "images"), taskAttemptContext);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException {
        return new MongoDBCommitter();
    }
}
