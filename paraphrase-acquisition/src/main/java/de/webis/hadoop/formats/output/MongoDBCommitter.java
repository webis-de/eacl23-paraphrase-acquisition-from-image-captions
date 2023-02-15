package de.webis.hadoop.formats.output;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MongoDBCommitter extends OutputCommitter {
    private FileOutputCommitter fileOutputCommitter;

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

        Path outDir = new Path(jobContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));
        FileSystem fileSystem = outDir.getFileSystem(jobContext.getConfiguration());
        if (!fileSystem.mkdirs(outDir)) {
            throw new IOException("Can not create output dir \"" + outDir + "\"");
        }

        fileOutputCommitter = new FileOutputCommitter(outDir, jobContext);

        Configuration configuration = jobContext.getConfiguration();
        String host = configuration.get("mongodb.host");
        int port = configuration.getInt("mongodb.port", -1);
        String database = configuration.get("mongodb.database");

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> imageCollection = mongoDatabase.getCollection("images");

        imageCollection.drop();

        mongoClient.close();
    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
        Path outDir = new Path(taskAttemptContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"));
        fileOutputCommitter = new FileOutputCommitter(outDir, taskAttemptContext);

        fileOutputCommitter.setupTask(taskAttemptContext);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
        Configuration configuration = taskAttemptContext.getConfiguration();

        MongoClient mongoClient = new MongoClient(
                configuration.get("mongodb.host"), configuration.getInt("mongodb.port", -1));
        MongoDatabase database = mongoClient.getDatabase(configuration.get("mongodb.database"));
        MongoCollection<Document> imageCollection = database.getCollection("images");

        Path imagesPath = getTaskPath(taskAttemptContext, "images");
        FileSystem fileSystem = imagesPath.getFileSystem(taskAttemptContext.getConfiguration());

        FSDataInputStream inputStream = fileSystem.open(imagesPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;

        Document setDocument;

        Document filter = new Document();
        Document update = new Document("$inc", new Document("num_references", 1));
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);

        while ((line = reader.readLine()) != null) {
            setDocument = Document.parse(line);

            filter.put("_id", setDocument.get("_id"));

            update.get("$inc", Document.class).put("num_captions", setDocument.getInteger("num_captions"));
            setDocument.remove("num_captions");
            update.put("$setOnInsert", setDocument);

            imageCollection.updateOne(filter, update, updateOptions);
        }

        reader.close();
        mongoClient.close();

        Path taskPath = getTaskPath(taskAttemptContext, "images");

        fileSystem.delete(taskPath, true);

        fileOutputCommitter.commitTask(taskAttemptContext);
    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
        Path taskPath = getTaskPath(taskAttemptContext, "images");

        FileSystem fileSystem = taskPath.getFileSystem(taskAttemptContext.getConfiguration());

        fileOutputCommitter.abortTask(taskAttemptContext);
    }

    public Path getTaskPath(TaskAttemptContext context, String name) {
        Path outDir = new Path(context.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"), "mongo");
        return new Path(outDir, name + "_" + context.getTaskAttemptID().toString());
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        Configuration configuration = jobContext.getConfiguration();
        String host = configuration.get("mongodb.host");
        int port = configuration.getInt("mongodb.port", -1);
        String database = configuration.get("mongodb.database");

        MongoClient mongoClient = new MongoClient(host, port);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        MongoCollection<Document> imageCollection = mongoDatabase.getCollection("images");

        System.out.println("UNIQUE IMAGES: " + imageCollection.count());

        mongoClient.close();

        Path mongoPath = new Path(jobContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"), "mongo");
        FileSystem fileSystem = FileSystem.get(jobContext.getConfiguration());
        fileSystem.delete(mongoPath, true);

        fileOutputCommitter.commitJob(jobContext);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
        System.out.println("ABORT JOB");

        Path mongoPath = new Path(jobContext.getConfiguration().get("mapreduce.output.fileoutputformat.outputdir"), "mongo");
        FileSystem fileSystem = FileSystem.get(jobContext.getConfiguration());
        fileSystem.delete(mongoPath, true);

        fileOutputCommitter.abortJob(jobContext, state);
    }
}
