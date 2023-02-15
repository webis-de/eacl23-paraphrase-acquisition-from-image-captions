package de.webis.hadoop.formats.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public interface SQLWritable<K> extends WritableComparable<K> {
    String VAL_DELIM = new String(new char[]{30, 30});
    byte[] VAL_DELIM_BYTES = VAL_DELIM.getBytes(StandardCharsets.US_ASCII);

    String ENTRY_DELIM = "\n";
    byte[] ENTRY_DELIM_BYTES = ENTRY_DELIM.getBytes(StandardCharsets.US_ASCII);


    void writeSQL(DataOutput... dataOutput) throws IOException;
}
