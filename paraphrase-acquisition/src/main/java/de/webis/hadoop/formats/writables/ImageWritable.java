package de.webis.hadoop.formats.writables;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class ImageWritable implements SQLWritable<ImageWritable> {
    private static final byte[] EMPTY_DATA = new byte[0];

    private final URIWritable imageUri;
    private BytesWritable data;
    private final IntWritable numCaptions;

    public ImageWritable() {
        imageUri = new URIWritable();
        data = new BytesWritable();
        numCaptions = new IntWritable();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        imageUri.write(dataOutput);
        data.write(dataOutput);
        numCaptions.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        imageUri.readFields(dataInput);
        data.readFields(dataInput);
        numCaptions.readFields(dataInput);
    }

    @Override
    public int compareTo(@NotNull ImageWritable imageWritable) {
        return imageUri.getURI().compareTo(imageWritable.imageUri.getURI());
    }

    @Override
    public void writeSQL(DataOutput... dataOutput) throws IOException {
        dataOutput[0].write(String.valueOf(this.hashCode()).getBytes(StandardCharsets.US_ASCII));
        dataOutput[0].write(VAL_DELIM_BYTES);
        dataOutput[0].write(this.imageUri.getURI().toASCIIString().getBytes(StandardCharsets.US_ASCII));
        dataOutput[0].write(VAL_DELIM_BYTES);
        String hexEncodedData = String.valueOf(Hex.encodeHex(data.getBytes()));
        dataOutput[0].write(hexEncodedData.getBytes(StandardCharsets.US_ASCII));
        dataOutput[0].write(ENTRY_DELIM_BYTES);
    }

    public byte[] getData() {
        if (data != null)
            return data.getBytes();

        return EMPTY_DATA;
    }

    public void setData(final InputStream byteStream) throws IOException {
        setData(IOUtils.toByteArray(byteStream));
    }

    public URI getImageUri() {
        return imageUri.getURI();
    }

    public void setImageUri(final URI imageUri) {
        this.imageUri.setURI(imageUri);
    }

    public void setData(final byte[] data) {
        this.data = new BytesWritable(data);
    }

    public void setNumCaptions(int numCaptions) {
        this.numCaptions.set(numCaptions);
    }

    public void clear() {
        imageUri.setURI(null);
        data = null;
    }

    @Override
    public int hashCode() {
        return imageUri.hashCode();
    }

    public int getNumCaptions() {
        return numCaptions.get();
    }
}
