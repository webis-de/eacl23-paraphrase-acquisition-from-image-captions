package de.webis.hadoop.mapper;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.image_processing.filter.ImageFilterHeuristic;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.bson.Document;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class ImageFilterMapper extends Mapper<ImageReferenceWritable, NullWritable, ImageReferenceWritable, NullWritable> {
    private int startStage;

    private final List<ImageFilterHeuristic> imageFilterHeuristics;


    private MongoClient mongoClient;
    private MongoCollection<Document> imageCollection;

    private final Document queryDocument;

    private MultipleOutputs<?, ?> multipleOutputs;

    public ImageFilterMapper() {
        imageFilterHeuristics = new LinkedList<>();

        queryDocument = new Document();
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();

        int numFilter = configuration.getInt("filter.images.count", -1);
        startStage = configuration.getInt("filter.stage.start", -1);

        if (numFilter == -1) {
            throw new IOException("Can't find \"filter.images.count\" option");
        }

        for (int i = 0; i < numFilter; i++) {
            Class<?> retrievedClass = configuration.getClass("filter.images." + i, null);

            if (retrievedClass == null) {
                throw new IOException("Unknown class for option \"filter.images." + i + "\"");
            }

            Class<? extends ImageFilterHeuristic> filterClass = (Class<? extends ImageFilterHeuristic>) retrievedClass;
            try {
                imageFilterHeuristics.add(filterClass.newInstance());
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
                throw new IOException("Can't instantiate filter class \"" + filterClass.getCanonicalName() + "\"");
            }
        }

        String host = configuration.get("mongodb.host");
        int port = configuration.getInt("mongodb.port", -1);
        String database = configuration.get("mongodb.database");

        mongoClient = new MongoClient(host, port);
        MongoDatabase mongoDatabase = mongoClient.getDatabase(database);
        imageCollection = mongoDatabase.getCollection("images");
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(ImageReferenceWritable imageReferenceWritable, NullWritable nullWritable, Context context) throws IOException, InterruptedException {
        multipleOutputs.write(String.valueOf(startStage), imageReferenceWritable, nullWritable, startStage + "/references");

        context.getCounter(TableCounter.A_REFERENCES_NO_FILTER).increment(1L);
        context.getCounter(TableCounter.A_CAPTIONS_NO_FILTER).increment(imageReferenceWritable.getCaptions().size());

        queryDocument.clear();
        queryDocument.put("_id", imageReferenceWritable.getID());
        Document imageDocument = imageCollection.find(queryDocument).first();

        boolean accept = true;

        for (int i = 0; i < imageFilterHeuristics.size(); i++) {
            if (!imageFilterHeuristics.get(i).accept(imageDocument)) {
                accept = false;
                break;
            } else {
                int currentStage = startStage + i + 1;
                multipleOutputs.write(String.valueOf(currentStage), imageReferenceWritable, nullWritable, currentStage + "/references");
            }
        }

        if (accept) {
            context.write(imageReferenceWritable, nullWritable);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mongoClient.close();
        multipleOutputs.close();
    }
}
