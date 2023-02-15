package de.webis.image_equality;

import de.webis.hadoop.formats.writables.ImageReferenceWritable;

public interface EqualityMetric {
    byte[] getFingerprint(ImageReferenceWritable imageReferenceWritable);
}
