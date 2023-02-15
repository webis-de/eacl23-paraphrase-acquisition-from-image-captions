package de.webis.hadoop.formats.input;

import com.ctc.wstx.api.WstxInputProperties;
import com.ctc.wstx.exc.WstxParsingException;
import com.ctc.wstx.stax.WstxInputFactory;
import de.webis.hadoop.counter.WebPageCounter;
import de.webis.hadoop.formats.writables.WikiRevisionWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class WikiDumpInputFormat extends FileInputFormat<LongWritable, WikiRevisionWritable> {
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    @Override
    public RecordReader<LongWritable, WikiRevisionWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        return new WikiRecordReader();
    }


    public static class WikiRecordReader extends RecordReader<LongWritable, WikiRevisionWritable> {
        private FileSplit fileSplit;

        private SplitCompressionInputStream compressionInputStream;

        private long startByte;
        private long length;

        private XMLStreamReader eventReader;

        private WikiRevisionWritable currentRevision;

        private org.apache.hadoop.mapreduce.Counter pageCounter;
        private org.apache.hadoop.mapreduce.Counter revisionCounter;


        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            fileSplit = (FileSplit) inputSplit;
            FileSystem fileSystem = fileSplit.getPath().getFileSystem(taskAttemptContext.getConfiguration());

            FSDataInputStream fsDataInputStream = fileSystem.open(fileSplit.getPath());

            startByte = fileSplit.getStart();
            length = fileSplit.getLength();
            long endByte = startByte + fileSplit.getLength();

            Decompressor decompressor = Bzip2Factory.getBzip2Decompressor(taskAttemptContext.getConfiguration());

            BZip2Codec codec = new BZip2Codec();

            compressionInputStream =
                    codec.createInputStream(
                            fsDataInputStream, decompressor,
                            startByte, endByte,
                            SplittableCompressionCodec.READ_MODE.BYBLOCK);

            startByte = compressionInputStream.getAdjustedStart();

            InputStreamReader inputStreamReader = new InputStreamReader(compressionInputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader);

            reader.mark(1000);
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.contains("<page>")) {
                    reader.reset();
                    break;
                }
                reader.mark(1000);
            }

            XMLInputFactory factory = WstxInputFactory.newFactory();
            factory.setProperty(WstxInputProperties.P_INPUT_PARSING_MODE, WstxInputProperties.PARSING_MODE_FRAGMENT);
            factory.setProperty(XMLInputFactory.IS_VALIDATING, false);
            factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
            factory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, false);
            factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

            try {
                eventReader = factory.createXMLStreamReader(reader);
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }

            currentRevision = new WikiRevisionWritable();

            pageCounter = taskAttemptContext.getCounter(WebPageCounter.PAGES);
            revisionCounter = taskAttemptContext.getCounter(WebPageCounter.REVISIONS);
        }

        @Override
        public boolean nextKeyValue() {
            boolean isRevision = false;

            try {
                while (eventReader.hasNext()) {
                    eventReader.next();

                    if (eventReader.isEndElement()) {
                        switch (eventReader.getName().getLocalPart()) {
                            case "revision":
                                isRevision = false;
                                if (currentRevision.getRevisionId() != -1) {
                                    return true;
                                }
                                break;
                            case "page":
                                if (compressionInputStream.getPos() - startByte >= length) {
                                    return false;
                                }
                                break;
                        }
                    } else if (eventReader.isStartElement()) {
                        switch (eventReader.getName().getLocalPart()) {
                            case "id":
                                if (!isRevision) {
                                    currentRevision.setPageId(Long.parseLong(eventReader.getElementText()));
                                } else {
                                    if (currentRevision.getRevisionId() == -1)
                                        currentRevision.setRevisionId(Long.parseLong(eventReader.getElementText()));
                                }
                                break;
                            case "title":
                                currentRevision.setTitle(eventReader.getElementText());
                                break;
                            case "revision":
                                currentRevision.setRevisionId(-1);
                                isRevision = true;
                                revisionCounter.increment(1L);
                                break;
                            case "page":
                                pageCounter.increment(1L);
                                break;
                            case "text":
                                if (currentRevision == null) {
                                    throw new XMLStreamException("Revision is null!");
                                }

                                currentRevision.setContent(eventReader.getElementText());
                                break;
                            case "ns":
                                currentRevision.setNs(Integer.parseInt(eventReader.getElementText()));
                                break;
                        }
                    }
                }
            } catch (WstxParsingException ignored) {
            } catch (IOException | XMLStreamException e) {
                e.printStackTrace();
            }

            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return currentRevision.getRevisionIdWritable();
        }

        @Override
        public WikiRevisionWritable getCurrentValue() {
            return currentRevision;
        }

        @Override
        public float getProgress() throws IOException {
            return (float) (compressionInputStream.getPos() - startByte) / (float) (fileSplit.getLength());
        }

        @Override
        public void close() {
            try {
                if (eventReader != null) {
                    eventReader.close();
                    eventReader = null;
                }

                if (compressionInputStream != null) {
                    compressionInputStream.close();
                    compressionInputStream = null;
                }
            } catch (IOException | XMLStreamException e) {
                e.printStackTrace();
            }
        }
    }
}
