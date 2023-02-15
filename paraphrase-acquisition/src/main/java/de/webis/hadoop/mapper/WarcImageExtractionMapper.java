package de.webis.hadoop.mapper;

import de.webis.caption_extraction.CaptionExtractor;
import de.webis.caption_extraction.HtmlAltTagExtractor;
import de.webis.hadoop.counter.ImageCounter;
import de.webis.hadoop.counter.WebPageCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ImageWritable;
import de.webis.parser.uri.URIParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.jsoup.Jsoup;
import org.jsoup.UncheckedIOException;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.netpreserve.jwarc.*;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

public class WarcImageExtractionMapper extends Mapper<LongWritable, Text, ImageWritable, NullWritable> {
    private final static CaptionExtractor CAPTION_EXTRACTOR = new HtmlAltTagExtractor();

    private final static WarcFilter RESPONSE_FILTER = WarcFilter.compile("warc-type == \"response\"");
    private final static WarcFilter IMAGE_FILTER = WarcFilter.compile("http:content-type =~ \"image/.*\"");
    private final static WarcFilter HTML_FILTER = WarcFilter.compile("http:content-type =~ \".*html.*\"");

    private final static Predicate<WarcRecord> IMAGE_OR_HTML_FILTER = IMAGE_FILTER.or(HTML_FILTER);

    private final ImageWritable imageWritable;
    private final ImageReferenceWritable imageReferenceWritable;
    private final NullWritable nullWritable;

    private MultipleOutputs<?, ?> multipleOutputs;

    public WarcImageExtractionMapper() {
        imageWritable = new ImageWritable();
        imageReferenceWritable = new ImageReferenceWritable();
        nullWritable = NullWritable.get();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text warcPath, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path path = new Path(warcPath.toString());

        FileSystem fileSystem = path.getFileSystem(conf);
        FSDataInputStream inputStream = fileSystem.open(path);
        WarcReader warcReader;

        try {
            warcReader = new WarcReader(inputStream);
        } catch (FileNotFoundException e) {
            return;
        }

        Iterator<WarcRecord> warcRecordIterator = warcReader.iterator();
        while (true) {
            try {
                if (!warcRecordIterator.hasNext()) {
                    break;
                }

                WarcRecord record = warcRecordIterator.next();

                if (RESPONSE_FILTER.test(record)) {
                    context.getCounter(WebPageCounter.PAGES).increment(1L);

                    if (IMAGE_OR_HTML_FILTER.test(record)) {

                        WarcResponse response = (WarcResponse) record;
                        int targetUriLength = response.targetURI().toASCIIString().length();

                        if (targetUriLength > 2000 || targetUriLength <= 11) {
                            continue;
                        }

                        Optional<WarcPayload> payload = response.payload();
                        if (payload.isPresent()) {
                            InputStream payloadStream = payload.get().body().stream();

                            try {
                                if (IMAGE_FILTER.test(record)) {
                                    handleImage(response.targetURI(), payloadStream, context);
                                } else if (HTML_FILTER.test(record)) {
                                    handleHTML(response.targetURI(), payloadStream, context);
                                }
                            } catch (IllegalArgumentException e) {
                                e.printStackTrace();
                            } catch (ParsingException | EOFException ignored) {
                            }

                            payloadStream.close();
                        }
                    }
                }

            } catch (java.io.UncheckedIOException e) {
                if ((e.getCause() instanceof EOFException)) {
                    break;
                }
            }
        }

        warcReader.close();
    }

    private void handleImage(URI targetURI, InputStream payload, Context context) throws IOException, InterruptedException {
        imageWritable.setImageUri(targetURI);
        imageWritable.setData(payload);

        context.write(imageWritable, nullWritable);
        context.getCounter(ImageCounter.PHYSICAL_IMAGES).increment(1L);
        imageWritable.clear();
    }

    private void handleHTML(URI targetURI, InputStream payload, Context context) throws IOException, InterruptedException {
        Document doc;

        try {
            doc = Jsoup.parse(payload, StandardCharsets.UTF_8.name(), targetURI.toString());
        } catch (NullPointerException | UncheckedIOException | IOException e) {
            System.err.println("Can't parse document: ");
            return;
        }


        Elements imgTags = doc.select("img");

        for (Element imgTag : imgTags) {
            if (!imgTag.hasAttr("src")) {
                continue;
            }

            String imgSrc = imgTag.attr("abs:src");
            URI imageURI;

            try {
                imageURI = URI.create(imgSrc);
            } catch (IllegalArgumentException e) {
                imgSrc = imgTag.attr("src");

                imageURI = URIParser.parse(targetURI, imgSrc);

                if (imageURI == null) {
                    continue;
                }
            }

            imageURI = imageURI.normalize();

            if (!imageURI.toString().isEmpty()) {
                int uriLength = imageURI.toASCIIString().length();
                if (uriLength <= 20 || uriLength > 2000) {
                    continue;
                }

                String imageTagString = imgTag.toString();
                imageTagString = imageTagString.replace('\n', ' ');

                imageReferenceWritable.setImageUri(imageURI);
                imageReferenceWritable.setPageUri(targetURI);
                imageReferenceWritable.setImageTag(imageTagString);

                String caption = CAPTION_EXTRACTOR.extract(imageReferenceWritable.getImageTag());
                if (caption != null && !caption.isEmpty())
                    imageReferenceWritable.addCaption(CAPTION_EXTRACTOR.getCaptionType(), caption);

                multipleOutputs.write("references", imageReferenceWritable, nullWritable);
                context.getCounter(ImageCounter.REFERENCES).increment(1L);

                imageReferenceWritable.clear();

                imageWritable.clear();
                imageWritable.setImageUri(imageURI);
                context.write(imageWritable, nullWritable);
                imageWritable.clear();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
