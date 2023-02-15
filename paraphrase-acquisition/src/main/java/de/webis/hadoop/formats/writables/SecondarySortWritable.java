package de.webis.hadoop.formats.writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SecondarySortWritable implements WritableComparable<SecondarySortWritable> {
    private String groupingCriteria;
    private String secondarySortCriteria;

    public SecondarySortWritable() {
        groupingCriteria = "";
        secondarySortCriteria = "";
    }

    public SecondarySortWritable(String groupingCriteria, String secondarySortCriteria) {
        this.groupingCriteria = groupingCriteria;
        this.secondarySortCriteria = secondarySortCriteria;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        byte[] groupingBytes = groupingCriteria.getBytes(StandardCharsets.UTF_8);

        dataOutput.writeInt(groupingBytes.length);
        dataOutput.write(groupingBytes);

        dataOutput.writeUTF(secondarySortCriteria);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        byte[] groupingBytes = new byte[dataInput.readInt()];

        dataInput.readFully(groupingBytes);
        groupingCriteria = new String(groupingBytes, StandardCharsets.UTF_8);
        secondarySortCriteria = dataInput.readUTF();
    }

    @Override
    public int compareTo(SecondarySortWritable secondarySortWritable) {
        int comparison = groupingCriteria.compareTo(secondarySortWritable.groupingCriteria);

        if (comparison == 0) {
            return secondarySortCriteria.compareTo(secondarySortWritable.secondarySortCriteria);
        }

        return comparison;
    }

    @Override
    public int hashCode() {
        return (groupingCriteria + "|" + secondarySortCriteria).hashCode();
    }

    public String getGroupingCriteria() {
        return groupingCriteria;
    }

    public String getSecondarySortCriteria() {
        return secondarySortCriteria;
    }

    public void setGroupingCriteria(String groupingCriteria) {
        this.groupingCriteria = groupingCriteria;
    }

    public void setSecondarySortCriteria(String secondarySortCriteria) {
        this.secondarySortCriteria = secondarySortCriteria;
    }

    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(SecondarySortWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SecondarySortWritable left = (SecondarySortWritable) a;
            SecondarySortWritable right = (SecondarySortWritable) b;

            return left.getGroupingCriteria().compareTo(right.getGroupingCriteria());
        }
    }

    public static class SortComparator extends WritableComparator {
        public SortComparator() {
            super(SecondarySortWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            SecondarySortWritable left = (SecondarySortWritable) a;
            SecondarySortWritable right = (SecondarySortWritable) b;

            return left.compareTo(right);
        }
    }

    public static class WritablePartitioner extends Partitioner<SecondarySortWritable, Writable> {
        @Override
        public int getPartition(SecondarySortWritable key, Writable value, int numPartitions) {
            return Math.floorMod(key.groupingCriteria.hashCode(), numPartitions);
        }
    }
}
