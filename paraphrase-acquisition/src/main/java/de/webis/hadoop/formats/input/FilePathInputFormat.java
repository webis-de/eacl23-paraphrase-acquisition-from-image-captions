package de.webis.hadoop.formats.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class FilePathInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new FilePathRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }


    public static class FilePathRecordReader extends RecordReader<LongWritable, Text> {
        private Path filePath;

        private boolean hasNext;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            filePath = ((FileSplit) inputSplit).getPath();
            hasNext = true;
        }

        @Override
        public boolean nextKeyValue() {
            if (hasNext) {
                hasNext = false;

                return true;
            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(filePath.hashCode());
        }

        @Override
        public Text getCurrentValue() {
            return new Text(filePath.toString());
        }

        @Override
        public float getProgress() {
            return hasNext ? 0.0f : 1.0f;
        }

        @Override
        public void close() {
        }
    }
}
