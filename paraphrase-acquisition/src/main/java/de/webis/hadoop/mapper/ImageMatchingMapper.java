package de.webis.hadoop.mapper;

import de.webis.hadoop.counter.PipelineCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.image_equality.EqualityMetric;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImageMatchingMapper extends Mapper<ImageReferenceWritable, NullWritable, BytesWritable, ImageReferenceWritable> {
    private EqualityMetric equalityMetric;

    private final BytesWritable fingerprint;

    public ImageMatchingMapper() {
        fingerprint = new BytesWritable();
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();

        Class<?> retrievedClass = configuration.getClass("matching.equality_metric", null);

        if (retrievedClass == null) {
            throw new IOException("Unknown class for option \"matching.equality_metric\"!");
        }

        Class<? extends EqualityMetric> equalityClass = (Class<? extends EqualityMetric>) retrievedClass;

        try {
            equalityMetric = equalityClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new IOException("Can't instantiate \"" + retrievedClass.getCanonicalName() + "\"!");
        }
    }

    @Override
    protected void map(ImageReferenceWritable imageReferenceWritable, NullWritable nullWritable, Context context) throws IOException, InterruptedException {
        context.getCounter(PipelineCounter.INPUT_IMAGE_MATCHER_REFERENCES).increment(1L);
        context.getCounter(PipelineCounter.INPUT_IMAGE_MATCHER_CAPTIONS).increment(imageReferenceWritable.getCaptions().size());
        byte[] fingerprintBytes = equalityMetric.getFingerprint(imageReferenceWritable);
        fingerprint.set(fingerprintBytes, 0, fingerprintBytes.length);

//        System.out.println(imageReferenceWritable.getImageUri().toASCIIString() + " | " + new String(fingerprint.getBytes()));
//        Thread.sleep(5000);

        context.write(fingerprint, imageReferenceWritable);
    }
}
