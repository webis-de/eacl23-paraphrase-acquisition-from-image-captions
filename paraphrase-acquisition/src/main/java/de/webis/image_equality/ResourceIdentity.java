package de.webis.image_equality;

import de.webis.hadoop.formats.writables.ImageReferenceWritable;

public class ResourceIdentity implements EqualityMetric {
    @Override
    public byte[] getFingerprint(ImageReferenceWritable imageReferenceWritable) {
        int id = imageReferenceWritable.getID();
        return new byte[]{
                (byte) (id >> 24 & 0xff),
                (byte) (id >> 16 & 0xff),
                (byte) (id >> 8 & 0xff),
                (byte) (id & 0xff)
        };
    }
}
