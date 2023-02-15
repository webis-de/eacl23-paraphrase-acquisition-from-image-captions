package de.webis.hadoop.mapper;

import de.webis.caption_extraction.CaptionExtractor;
import de.webis.caption_extraction.CaptionType;
import de.webis.caption_extraction.WikiAltTagExtractor;
import de.webis.caption_extraction.WikiTextExtractor;
import de.webis.hadoop.counter.ImageCounter;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ImageWritable;
import de.webis.hadoop.formats.writables.WikiRevisionWritable;
import de.webis.parser.wikitext.WikiTextImageExtractor;
import org.apache.commons.lang.CharEncoding;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiImageExtractionMapper extends Mapper<LongWritable, WikiRevisionWritable, ImageWritable, NullWritable> {
    private static final URI WIKI_COMMONS_DOMAIN = URI.create("https://commons.wikimedia.org/wiki/");
    private final static URI EN_WIKI_DOMAIN = URI.create("https://en.wikipedia.org/wiki/");
    private final static URI SIMPLE_WIKI_DOMAIN = URI.create("https://simple.wikipedia.org/wiki/");

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("(?:File|Image):[^|\\]]+");

    private WikiTextImageExtractor imageExtractor;

    private final ImageReferenceWritable referenceWritable;
    private final ImageWritable imageWritable;
    private final NullWritable nullOut;

    private final CaptionExtractor captionExtractor;
    private final CaptionExtractor altTagCaptionExtractor;

    private MultipleOutputs<?, ?> multipleOutputs;

    public WikiImageExtractionMapper() {
        referenceWritable = new ImageReferenceWritable();
        imageWritable = new ImageWritable();
        nullOut = NullWritable.get();

        captionExtractor = new WikiTextExtractor();
        altTagCaptionExtractor = new WikiAltTagExtractor();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        imageExtractor = new WikiTextImageExtractor(context);

        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, WikiRevisionWritable wikiRevision, Context context) throws IOException, InterruptedException {
        if(wikiRevision.getNs() != 0){
            return;
        }

        imageExtractor.reset(wikiRevision.getContent());

        while (imageExtractor.next()) {
            String wikiFileTag = imageExtractor.getCurrent();

            Matcher fileNameMatcher = FILE_NAME_PATTERN.matcher(wikiFileTag);
            String filename = null;

            if (fileNameMatcher.find()) {
                filename = fileNameMatcher.group(0);
            }

            if (filename == null) {
                continue;
            }

            filename = filename.replace("Image:", "File:");

            String extractedCaption = captionExtractor.extract(wikiFileTag);
            String altTagCaption = altTagCaptionExtractor.extract(wikiFileTag);

            extractedCaption = StringUtils.stripAccents(extractedCaption);
            altTagCaption = StringUtils.stripAccents(altTagCaption);

            URI imageURI = WIKI_COMMONS_DOMAIN.resolve(
                    URLEncoder.encode(filename, CharEncoding.UTF_8).replaceAll("\\+", "_"));

            URI pageURI;
            if(context.getConfiguration().get("mongodb.database").equals("wikipedia-simple")){
                pageURI = SIMPLE_WIKI_DOMAIN.resolve(
                        URLEncoder.encode(wikiRevision.getTitle(), CharEncoding.UTF_8).replaceAll("\\+", "_"));
            } else {
                pageURI = EN_WIKI_DOMAIN.resolve(
                        URLEncoder.encode(wikiRevision.getTitle(), CharEncoding.UTF_8).replaceAll("\\+", "_"));
            }


            referenceWritable.setImageUri(imageURI);
            referenceWritable.setPageUri(pageURI);
            referenceWritable.setImageTag(wikiFileTag.replace('\n', ' '));

            if (extractedCaption != null) {
                referenceWritable.addCaption(CaptionType.WIKI_TEXT, extractedCaption);
            }

            if (altTagCaption != null) {
                referenceWritable.addCaption(CaptionType.WIKI_TEXT_ALT_TAG, altTagCaption);
            }

            if (!referenceWritable.getCaptions().isEmpty()) {
                context.getCounter(ImageCounter.REFERENCE_WITH_CAPTION).increment(1L);
            }

            multipleOutputs.write("references", referenceWritable, nullOut);
            context.getCounter(ImageCounter.REFERENCES).increment(1L);
            context.getCounter(ImageCounter.CAPTIONS).increment(referenceWritable.getCaptions().size());

            imageWritable.setImageUri(imageURI);
            imageWritable.setNumCaptions(referenceWritable.getCaptions().size());
            context.write(imageWritable, nullOut);

            referenceWritable.clear();
            imageWritable.clear();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
