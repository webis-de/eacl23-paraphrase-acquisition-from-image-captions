package de.webis.hadoop.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import de.webis.image_equality.EqualityMetric;
import de.webis.image_processing.filter.ImageFilterHeuristic;
import de.webis.nlp.filter.caption.CaptionFilterHeuristic;
import de.webis.nlp.filter.paraphrase.ParaphraseFilterHeuristic;
import de.webis.nlp.preprocessor.Preprocessor;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CorpusConfig {
    @JsonProperty("name")
    private String name;

    @JsonProperty("filetype")
    private String filetype;

    @JsonProperty("filepathfilter")
    private Class<? extends PathFilter> filePathFilter;

    @JsonProperty("remote_filepaths")
    private List<String> remoteFilePaths;

    @JsonProperty("local_filepaths")
    private List<String> localFilePaths;

    private String basePath;

    private boolean applyImageFilter;
    private List<Class<? extends ImageFilterHeuristic>> imageFilterHeuristics;
    private List<Class<? extends Preprocessor>> captionPreprocessors;
    private List<Class<? extends CaptionFilterHeuristic>> captionFilterHeuristics;
    private Class<? extends EqualityMetric> imageEqualityClass;
    private List<Class<? extends ParaphraseFilterHeuristic>> paraphraseFilterHeuristics;

    private long imageExtractionBatchMapperMemory, imageExtractionMapperMemory,
            imageExtractionBatchReducerMemory, imageExtractionReducerMemory;
    private long imageExtractionNumReducer;

    private int numBatches;

    public CorpusConfig() {
    }

    public CorpusConfig(String name, String filetype, Class<? extends PathFilter> filePathFilter, List<String> remoteFilePaths, List<String> localFilePaths) {
        this.name = name;
        this.filetype = filetype;
        this.filePathFilter = filePathFilter;
        this.remoteFilePaths = remoteFilePaths;
        this.localFilePaths = localFilePaths;
    }

    public String getName() {
        return name;
    }

    public String getFiletype() {
        return filetype;
    }

    public Class<? extends PathFilter> getFilePathFilter() {
        return filePathFilter;
    }

    public List<String> getRemoteFilePaths() {
        return remoteFilePaths;
    }

    public List<String> getLocalFilePaths() {
        return localFilePaths;
    }

    public String getBasePath() {
        return basePath;
    }

    public boolean isApplyImageFilter() {
        return applyImageFilter;
    }

    public List<Class<? extends ImageFilterHeuristic>> getImageFilterHeuristics() {
        return imageFilterHeuristics;
    }

    public List<Class<? extends Preprocessor>> getCaptionPreprocessors() {
        return captionPreprocessors;
    }

    public List<Class<? extends CaptionFilterHeuristic>> getCaptionFilterHeuristics() {
        return captionFilterHeuristics;
    }

    public Class<? extends EqualityMetric> getImageEqualityClass() {
        return imageEqualityClass;
    }

    public List<Class<? extends ParaphraseFilterHeuristic>> getParaphraseFilterHeuristics() {
        return paraphraseFilterHeuristics;
    }

    public long getImageExtractionMapperMemory() {
        return imageExtractionMapperMemory;
    }

    public long getImageExtractionReducerMemory() {
        return imageExtractionReducerMemory;
    }

    public long getImageExtractionNumReducer() {
        return imageExtractionNumReducer;
    }

    public long getImageExtractionBatchMapperMemory() {
        return imageExtractionBatchMapperMemory;
    }

    public long getImageExtractionBatchReducerMemory() {
        return imageExtractionBatchReducerMemory;
    }

    public int getNumBatches() {
        return numBatches;
    }

    @Override
    public String toString() {
        return "CorpusConfig{" +
                "name='" + name + '\'' +
                ", filetype='" + filetype + '\'' +
                ", filePathFilter=" + filePathFilter +
                ", remoteFilePaths=" + remoteFilePaths +
                ", localFilePaths=" + localFilePaths +
                ", basePath='" + basePath + '\'' +
                '}';
    }
    @JsonProperty("in")
    @SuppressWarnings("unchecked")
    private void unpackInProperties(Map<String, Object> in) throws ClassNotFoundException {
        filetype = (String) in.get("filetype");
        try {
            filePathFilter = Class.forName((String) in.get("filepathfilter")).asSubclass(PathFilter.class);
        } catch (NullPointerException e) {
            filePathFilter = null;
        }

        remoteFilePaths = (List<String>) in.get("remote_filepaths");
        localFilePaths = (List<String>) in.get("local_filepaths");
    }

    @JsonProperty("out")
    private void unpackOutProperties(Map<String, String> out) {
        basePath = out.get("base_path");
    }

    @JsonProperty("image_extraction")
    private void unpackImageExtractionProperties(Map<String, String> imageExtraction) {
        imageExtractionBatchMapperMemory = Long.parseLong(imageExtraction.get("batch_mapper_memory"));
        imageExtractionBatchReducerMemory = Long.parseLong(imageExtraction.get("batch_reducer_memory"));

        imageExtractionMapperMemory = Long.parseLong(imageExtraction.get("mapper_memory"));
        imageExtractionReducerMemory = Long.parseLong(imageExtraction.get("reducer_memory"));

        imageExtractionNumReducer = Long.parseLong(imageExtraction.get("reducer_num"));

        numBatches = Integer.parseInt(imageExtraction.get("num_batches"));
    }

    @JsonProperty("paraphrasing")
    @SuppressWarnings("unchecked")
    private void unpackParaphraseProperties(Map<String, Object> paraphrasing) throws ClassNotFoundException {
        applyImageFilter = (boolean) paraphrasing.get("apply_img_filter");
        imageFilterHeuristics = new LinkedList<>();

        for (String className : (List<String>) paraphrasing.get("img_filter")) {
            imageFilterHeuristics.add(Class.forName(className).asSubclass(ImageFilterHeuristic.class));
        }

        captionPreprocessors = new LinkedList<>();

        for (String className : (List<String>) paraphrasing.get("caption_preprocessor")) {
            captionPreprocessors.add(Class.forName(className).asSubclass(Preprocessor.class));
        }

        captionFilterHeuristics = new LinkedList<>();

        for (String className : (List<String>) paraphrasing.get("caption_filter")) {
            captionFilterHeuristics.add(Class.forName(className).asSubclass(CaptionFilterHeuristic.class));
        }

        imageEqualityClass = Class.forName((String) paraphrasing.get("img_equality_class")).asSubclass(EqualityMetric.class);
        paraphraseFilterHeuristics = new LinkedList<>();

        for (String className : (List<String>) paraphrasing.get("paraphrase_filter")) {
            paraphraseFilterHeuristics.add(Class.forName(className).asSubclass(ParaphraseFilterHeuristic.class));
        }
    }

    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        YAMLParser parser = new YAMLFactory().createParser(CorpusConfig.class.getResource("/corpora.yaml"));
        List<CorpusConfig> corpora = objectMapper.readValues(parser, CorpusConfig.class).readAll();
        for (CorpusConfig corpusConfig : corpora) {
            System.out.println(corpusConfig.getImageFilterHeuristics().get(0).getCanonicalName());
        }
    }
}
