package de.webis.hadoop.formats.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

public class URIWritable implements Writable {
    private URI uri;

    public URIWritable() {
        uri = null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBoolean(uri != null);
        if (uri != null) {
            byte[] uriBytes = uri.toString().getBytes(StandardCharsets.UTF_8);
            dataOutput.writeInt(uriBytes.length);
            dataOutput.write(uriBytes);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            byte[] uriBytes = new byte[dataInput.readInt()];
            dataInput.readFully(uriBytes);
            uri = URI.create(new String(uriBytes, StandardCharsets.UTF_8));
        } else {
            uri = null;
        }
    }

    public URI getURI() {
        return uri;
    }

    public void setURI(URI uri) {
        this.uri = uri;
    }

    @Override
    public String toString() {
        return "URIWritable{" +
                "uri=" + uri +
                '}';
    }

    @Override
    public int hashCode() {
        return uri.toASCIIString().hashCode();
    }
}
