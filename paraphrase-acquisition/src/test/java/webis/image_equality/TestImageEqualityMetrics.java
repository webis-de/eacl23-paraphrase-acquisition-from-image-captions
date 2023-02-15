package webis.image_equality;

import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.image_equality.EqualityMetric;
import de.webis.image_equality.ResourceIdentity;
import org.apache.hadoop.io.BytesWritable;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

public class TestImageEqualityMetrics {

    @Test
    public void testResourceIdentity() {
        EqualityMetric equalityMetric = new ResourceIdentity();

        ImageReferenceWritable imageReferenceWritable = new ImageReferenceWritable();
        imageReferenceWritable.setImageUri(URI.create("http://this.test.org/image.png"));
        BytesWritable fingerprint1 = new BytesWritable(equalityMetric.getFingerprint(imageReferenceWritable));

        imageReferenceWritable.clear();
        imageReferenceWritable.setImageUri(URI.create("http://this.test.org/image.png"));
        BytesWritable fingerprint2 = new BytesWritable(equalityMetric.getFingerprint(imageReferenceWritable));

        String string1 = new String(fingerprint1.getBytes());
        String string2 = new String(fingerprint2.getBytes());

        assertEquals(string1, string2);
        assertTrue(Arrays.equals(fingerprint1.getBytes(), fingerprint2.getBytes()));
        assertEquals(fingerprint1, fingerprint2);

        imageReferenceWritable.setImageUri(URI.create("http://this.test.org/"));
        BytesWritable fingerprint3 = new BytesWritable(equalityMetric.getFingerprint(imageReferenceWritable));
        String string3 = new String(fingerprint3.getBytes());

        assertNotEquals(string1, string3);
        assertNotEquals(string2, string3);

    }
}
