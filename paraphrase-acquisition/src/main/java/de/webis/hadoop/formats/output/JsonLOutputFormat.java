package de.webis.hadoop.formats.output;

import com.fasterxml.jackson.databind.JsonSerializable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonLOutputFormat extends FileOutputFormat<JsonSerializable, NullWritable> {
    @Override
    public RecordWriter<JsonSerializable, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new JsonLOutputWriter(getDefaultWorkFile(taskAttemptContext, ".jsonl"), taskAttemptContext);
    }

    static class JsonLOutputWriter extends RecordWriter<JsonSerializable, NullWritable> {
        private final FSDataOutputStream dataOutputStream;
        private final ObjectMapper jsonMapper;


        public JsonLOutputWriter(Path defaultWorkFile, TaskAttemptContext taskAttemptContext) throws IOException {
            FileSystem fileSystem = defaultWorkFile.getFileSystem(taskAttemptContext.getConfiguration());

            dataOutputStream = fileSystem.create(defaultWorkFile);
            jsonMapper = new ObjectMapper();
        }

        @Override
        public void write(JsonSerializable jsonSerializable, NullWritable nullWritable) throws IOException, InterruptedException {
            dataOutputStream.write(jsonMapper.writeValueAsString(jsonSerializable).getBytes(StandardCharsets.UTF_8));
            dataOutputStream.write("\n".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            dataOutputStream.close();
        }
    }
}
