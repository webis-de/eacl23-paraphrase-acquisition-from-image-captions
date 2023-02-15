package de.webis.hadoop.formats.output;

import de.webis.hadoop.formats.writables.ImageWritable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;
import org.bson.types.Binary;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class MongoDBWriter extends RecordWriter<ImageWritable, NullWritable> {
    private final static JsonWriterSettings JSON_WRITER_SETTINGS = new JsonWriterSettings();
    private final FSDataOutputStream outputStream;
    private final Document outDocument;

    public MongoDBWriter(Path outPath, TaskAttemptContext context) throws IOException {
        FileSystem fileSystem = outPath.getFileSystem(context.getConfiguration());
        outputStream = fileSystem.create(outPath);
        outDocument = new Document();
    }

    @Override
    public void write(ImageWritable imageWritable, NullWritable nullWritable) throws IOException, InterruptedException {
        outDocument.clear();

        outDocument.put("_id", imageWritable.hashCode());
        outDocument.put("image_uri", imageWritable.getImageUri().toASCIIString());
        byte[] data = imageWritable.getData();

        if (data.length != 0) {
            Binary binary = new Binary(data);
            outDocument.put("data", binary);
        }

        outDocument.put("num_captions", imageWritable.getNumCaptions());
        outputStream.write(outDocument.toJson(JSON_WRITER_SETTINGS).getBytes(StandardCharsets.UTF_8));
        outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        outputStream.close();
    }
}
