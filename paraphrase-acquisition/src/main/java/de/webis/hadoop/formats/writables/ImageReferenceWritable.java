package de.webis.hadoop.formats.writables;

import de.webis.caption_extraction.CaptionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class ImageReferenceWritable implements Writable {
    private final URIWritable imageUri;
    private final URIWritable pageUri;
    private final Text imageTag;
    private final Map<CaptionType, String> captions;

    public ImageReferenceWritable() {
        imageUri = new URIWritable();
        pageUri = new URIWritable();
        imageTag = new Text();
        captions = new HashMap<>();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        imageUri.write(dataOutput);

        pageUri.write(dataOutput);
        imageTag.write(dataOutput);

        dataOutput.writeInt(captions.size());
        for (Map.Entry<CaptionType, String> caption : captions.entrySet()) {
            dataOutput.writeUTF(caption.getKey().name());
            byte[] captionBytes = caption.getValue().getBytes(StandardCharsets.UTF_8);
            dataOutput.writeInt(captionBytes.length);
            dataOutput.write(captionBytes);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        imageUri.readFields(dataInput);

        pageUri.readFields(dataInput);
        imageTag.readFields(dataInput);

        captions.clear();
        int numCaptions = dataInput.readInt();
        for (int i = 0; i < numCaptions; i++) {
            CaptionType captionType = CaptionType.valueOf(dataInput.readUTF());
            byte[] captionBytes = new byte[dataInput.readInt()];
            dataInput.readFully(captionBytes);
            captions.put(captionType, new String(captionBytes, StandardCharsets.UTF_8));
        }
    }

    public URI getImageUri() {
        return imageUri.getURI();
    }

    public void setImageUri(URI imageUri) {
        this.imageUri.setURI(imageUri);
    }

    public String getImageTag() {
        return imageTag.toString();
    }

    public void addCaption(CaptionType captionType, String content) {
        captions.put(captionType, content);
    }

    public URI getPageUri() {
        return pageUri.getURI();
    }

    public void setPageUri(URI uri) {
        this.pageUri.setURI(uri);
    }

    public void setImageTag(String imageTag) {
        this.imageTag.set(imageTag);
    }

    public Map<CaptionType, String> getCaptions() {
        return captions;
    }

    public int getID() {
        return imageUri.hashCode();
    }

    public void clear() {
        imageUri.setURI(null);
        pageUri.setURI(null);
        imageTag.clear();
        captions.clear();
    }

    @Override
    public String toString() {
        return "ImageInstanceWritable{" +
                "uri=" + imageUri.getURI() +
                ", source=" + pageUri.getURI() +
                ", rawTag=" + imageTag +
                ", captions=" + captions +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof ImageReferenceWritable) {
            return hashCode() == o.hashCode();
        }

        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return imageUri.hashCode() ^ pageUri.hashCode() ^ captions.hashCode();
    }
}
