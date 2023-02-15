package de.webis.hadoop.formats.writables;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WikiRevisionWritable implements Writable {
    private final LongWritable pageId;
    private final LongWritable revisionId;

    private final IntWritable ns;

    private final Text title;
    private final Text content;

    public WikiRevisionWritable() {
        pageId = new LongWritable(-1L);
        revisionId = new LongWritable(-1L);

        ns = new IntWritable();

        title = new Text();
        content = new Text();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        pageId.write(dataOutput);
        revisionId.write(dataOutput);

        ns.write(dataOutput);

        title.write(dataOutput);
        content.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        pageId.readFields(dataInput);
        revisionId.readFields(dataInput);

        ns.readFields(dataInput);

        title.readFields(dataInput);
        content.readFields(dataInput);
    }

    public long getPageId() {
        return pageId.get();
    }

    public void setPageId(long pageId) {
        this.pageId.set(pageId);
    }

    public long getRevisionId() {
        return revisionId.get();
    }

    public void setRevisionId(long revisionId) {
        this.revisionId.set(revisionId);
    }

    public LongWritable getRevisionIdWritable() {
        return revisionId;
    }

    public String getTitle() {
        return title.toString();
    }

    public void setTitle(String title) {
        this.title.set(title);
    }

    public String getContent() {
        return content.toString();
    }

    public void setContent(String content) {
        this.content.set(content);
    }

    public void setNs(int ns){
        this.ns.set(ns);
    }

    public int getNs() {
        return ns.get();
    }

    @Override
    public String toString() {
        return "WikiRevisionWritable{" +
                "pageId=" + pageId +
                ", revisionId=" + revisionId +
                ", title=" + title +
//                ", content=" + content +
                '}';
    }
}
