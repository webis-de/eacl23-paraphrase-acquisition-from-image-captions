package de.webis.hadoop.jobs;

import com.fasterxml.jackson.databind.JsonSerializable;
import de.webis.hadoop.conf.CorpusConfig;
import de.webis.hadoop.counter.TableCounter;
import de.webis.hadoop.formats.output.JsonLOutputFormat;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import de.webis.hadoop.mapper.*;
import de.webis.hadoop.reducer.ParaphraseConstructionReducer;
import de.webis.hadoop.reducer.ParaphraseCounterReducer;
import de.webis.image_equality.EqualityMetric;
import de.webis.image_processing.filter.ImageFilterHeuristic;
import de.webis.nlp.filter.caption.CaptionFilterHeuristic;
import de.webis.nlp.filter.paraphrase.ParaphraseFilterHeuristic;
import de.webis.nlp.preprocessor.Preprocessor;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParaphraseAcquisitionJob extends HadoopJob {
    private final Map<String, String> stageNames;

    public ParaphraseAcquisitionJob(CorpusConfig corpus, boolean local) {
        super(corpus, local);

        stageNames = new HashMap<>();
        stageNames.put("0", "No filter");
        int currentStage = 1;

        for (Class<? extends ImageFilterHeuristic> filterHeuristic : corpus.getImageFilterHeuristics()) {
            stageNames.put(String.valueOf(currentStage), filterHeuristic.getSimpleName());
            currentStage++;
        }

        for (Class<? extends CaptionFilterHeuristic> filterHeuristic : corpus.getCaptionFilterHeuristics()) {
            stageNames.put(String.valueOf(currentStage), filterHeuristic.getSimpleName());
            currentStage++;
        }

        stageNames.put(String.valueOf(currentStage), "Paraphrase construction");
        currentStage++;

        stageNames.put(String.valueOf(currentStage), "Unique candidate");
        currentStage++;

        for (Class<? extends ParaphraseFilterHeuristic> filterHeuristic : corpus.getParaphraseFilterHeuristics()) {
            stageNames.put(String.valueOf(currentStage), filterHeuristic.getSimpleName());
            currentStage++;
        }

        System.out.println(stageNames);
    }

    @Override
    protected Job configureJob(String[] args) throws IOException {
        final JobConf jobConf = new JobConf();

        final long mapMemory = 8192;
        final long reduceMemory = 8192;

        jobConf.setMemoryForMapTask(mapMemory);
        jobConf.setMemoryForReduceTask(reduceMemory);

        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        jobConf.set("mongodb.host", args[0]);
        jobConf.setInt("mongodb.port", Integer.parseInt(args[1]));
        jobConf.set("mongodb.database", corpus.getName());

        final Job job = Job.getInstance(jobConf);
        job.setJobName("paraphrase-acquisition-" + corpus.getName());
        job.setJarByClass(ParaphraseAcquisitionJob.class);

        Path inPath = new Path(corpus.getBasePath(), "image-references");
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, inPath);

        ChainMapper.addMapper(
                job, ImageFilterMapper.class,
                ImageReferenceWritable.class, NullWritable.class,
                ImageReferenceWritable.class, NullWritable.class,
                getImgFilterConf()
        );

        ChainMapper.addMapper(
                job, CaptionFilterMapper.class,
                ImageReferenceWritable.class, NullWritable.class,
                ImageReferenceWritable.class, NullWritable.class,
                getCaptionFilterConf()
        );

        ChainMapper.addMapper(
                job, ImageMatchingMapper.class,
                ImageReferenceWritable.class, NullWritable.class,
                BytesWritable.class, ImageReferenceWritable.class,
                getImageMatchingConf()
        );


        ChainReducer.setReducer(
                job, ParaphraseConstructionReducer.class,
                BytesWritable.class, ImageReferenceWritable.class,
                IntWritable.class, ParaphraseWritable.class,
                getParaphraseConstructionConf());

        ChainReducer.addMapper(job, ParaphraseFilterMapper.class,
                IntWritable.class, ParaphraseWritable.class,
                IntWritable.class, ParaphraseWritable.class,
                getParaphraseFilterConf());

//        ChainReducer.addMapper(job, ParaphraseAssessmentMapper.class,
//                IntWritable.class, ParaphraseWritable.class,
//                IntWritable.class, ParaphraseWritable.class,
//                new Configuration(false));

        ChainReducer.addMapper(job, ParaphraseOutputMapper.class,
                IntWritable.class, ParaphraseWritable.class,
                IntWritable.class, ParaphraseWritable.class,
                new Configuration(false));

        job.setNumReduceTasks(10000);

        Path outPath = new Path(corpus.getBasePath(), "paraphrases");
        FileOutputFormat.setOutputPath(job, outPath);

        for (String stage : stageNames.keySet()) {
            MultipleOutputs.addNamedOutput(job, stage,
                    SequenceFileOutputFormat.class,
                    ImageReferenceWritable.class, NullWritable.class);
        }

        MultipleOutputs.addNamedOutput(job, "paraphrases", JsonLOutputFormat.class, JsonSerializable.class, NullWritable.class);
        MultipleOutputs.setCountersEnabled(job, true);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(ParaphraseWritable.class);

        return job;
    }

    private Job configureAnalysisJob(String stage) throws IOException {
        final JobConf jobConf = new JobConf();

        final long mapMemory = 4096;
        final long reduceMemory = 4096;

        jobConf.setMemoryForMapTask(mapMemory);
        jobConf.setMemoryForReduceTask(reduceMemory);

        final Job job = Job.getInstance(jobConf);
        job.setJobName("paraphrase-acquisition-analysis-" + corpus.getName() + "-" + stage);
        job.setJarByClass(ParaphraseAcquisitionJob.class);

        Path inPath = new Path(corpus.getBasePath(), "paraphrases/" + stage);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, inPath);

        ChainMapper.addMapper(job, ReferenceInputMapper.class,
                ImageReferenceWritable.class, NullWritable.class,
                ImageReferenceWritable.class, NullWritable.class,
                new Configuration(false));

        ChainMapper.addMapper(
                job, ImageMatchingMapper.class,
                ImageReferenceWritable.class, NullWritable.class,
                BytesWritable.class, ImageReferenceWritable.class,
                getImageMatchingConf()
        );

        ChainReducer.setReducer(
                job, ParaphraseCounterReducer.class,
                BytesWritable.class, ImageReferenceWritable.class,
                NullWritable.class, NullWritable.class,
                getParaphraseConstructionConf());

//        ChainReducer.addMapper(
//                job, ParaphraseCounterMapper.class,
//                IntWritable.class, ParaphraseWritable.class,
//                NullWritable.class, NullWritable.class,
//                new Configuration(false)
//        );

        job.setNumReduceTasks(10);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Path outPath = new Path(corpus.getBasePath(), "paraphrases/" + stage + "/analysis");
        FileOutputFormat.setOutputPath(job, outPath);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = configureJob(args);

        Path outputPath = FileOutputFormat.getOutputPath(job);
        FileSystem fileSystem = outputPath.getFileSystem(job.getConfiguration());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

//        return job.waitForCompletion(true)? 0 : 1;

        if (job.waitForCompletion(true)) {
            Object[] tableData = new Object[stageNames.size() * 12];
            int i = 0;
            Counters jobCounters = job.getCounters();

            for (Map.Entry<String, String> stage : stageNames.entrySet()) {
                Job analysisJob = configureAnalysisJob(stage.getKey());

                analysisJob.waitForCompletion(false);

                Counters analysisCounters = analysisJob.getCounters();
                tableData[i] = stage.getKey() + " " + stage.getValue();
                i++;
                tableData[i] = analysisCounters.findCounter(TableCounter.A_IMAGES_NO_FILTER).getValue();
                i++;
                if ((i - 13) > 0) {
                    tableData[i] = (1.0 - ((Long) tableData[i - 1]).doubleValue() / ((Long) tableData[i - 13]).doubleValue()) * -100.0;
                } else {
                    tableData[i] = 100.0;
                }
                i++;
                tableData[i] = analysisCounters.findCounter(TableCounter.A_REFERENCES_GEQ_2).getValue();
                i++;
                tableData[i] = analysisCounters.findCounter(TableCounter.A_REFERENCES_GEQ_5).getValue();
                i++;

                tableData[i] = analysisCounters.findCounter(TableCounter.A_REFERENCES_NO_FILTER).getValue();
                i++;
                if ((i - 13) > 0) {
                    tableData[i] = (1.0 - ((Long) tableData[i - 1]).doubleValue() / ((Long) tableData[i - 13]).doubleValue()) * -100.0;
                } else {
                    tableData[i] = 100.0;
                }
                i++;
                tableData[i] = ((Long) tableData[i - 2]).doubleValue() / ((Long) tableData[i - 6]).doubleValue();
                i++;
                tableData[i] = analysisCounters.findCounter(TableCounter.A_CAPTIONS_NO_FILTER).getValue();
                i++;
                if ((i - 13) > 0) {
                    tableData[i] = (1.0 - ((Long) tableData[i - 1]).doubleValue() / ((Long) tableData[i - 13]).doubleValue()) * -100.0;
                } else {
                    tableData[i] = 100.0;
                }
                i++;
                tableData[i] = analysisCounters.findCounter(TableCounter.A_PARAPHRASE_PAIRS_NO_FILTER).getValue();
                i++;
                if ((i - 13) > 0) {
                    tableData[i] = (1.0 - ((Long) tableData[i - 1]).doubleValue() / ((Long) tableData[i - 13]).doubleValue()) * -100.0;
                } else {
                    tableData[i] = 100.0;
                }
                i++;
            }

            final Object[] tableHeader = new Object[]{
                    "Stage", "Images", "Delta", ">=2", ">=5", "References", "Delta", "mu", "Captions", "Delta", "Paraphrases", "Delta"
            };

            int lineLength = 200;
            char[] line = new char[lineLength];
            Arrays.fill(line, '-');
            String lineStr = new String(line);

            StringBuilder formatStringBuilder = new StringBuilder();
            formatStringBuilder.append(lineStr).append("\n");
            formatStringBuilder.append("|%30s|%15s|%15s|%15s|%15s|%15s|%15s|%15s|%15s|%15s|%15s|%15s|\n");
            formatStringBuilder.append(lineStr).append("\n");
            for (String ignored : stageNames.keySet()) {
                formatStringBuilder.append("|%30s|%15d|%14.2f%%|%15d|%15d|%15d|%14.2f%%|%15.2f|%15d|%14.2f%%|%15d|%14.2f%%|\n");
            }
            formatStringBuilder.append(lineStr);

            System.out.println();
            System.out.printf(
                    formatStringBuilder.toString(),
                    ArrayUtils.addAll(tableHeader, tableData));
            System.out.println();

            PrintWriter writer = new PrintWriter(new FileWriter("data/paraphrases-" + corpus.getName() + "-stats.txt"));
            writer.printf(formatStringBuilder.toString(),
                    ArrayUtils.addAll(tableHeader, tableData));
            writer.close();

            return 0;
        }

        return 1;
    }

    private Configuration getImgFilterConf() {
        final Configuration imgFilterConf = new Configuration(false);

        List<Class<? extends ImageFilterHeuristic>> imageFilters = corpus.getImageFilterHeuristics();

        imgFilterConf.setInt("filter.stage.start", 0);
        imgFilterConf.setBoolean("filter.images.mandatory", corpus.isApplyImageFilter());
        imgFilterConf.setInt("filter.images.count", imageFilters.size());

        for (int i = 0; i < imageFilters.size(); i++) {
            imgFilterConf.setClass("filter.images." + i, imageFilters.get(i), ImageFilterHeuristic.class);
        }


        return imgFilterConf;
    }

    private Configuration getCaptionFilterConf() {
        final Configuration captionFilterConf = new Configuration(false);

        List<Class<? extends Preprocessor>> captionPreprocessors = corpus.getCaptionPreprocessors();

        captionFilterConf.setInt("filter.stage.start", 1 + corpus.getImageFilterHeuristics().size());
        captionFilterConf.setInt("filter.captions.pre.count", captionPreprocessors.size());

        for (int i = 0; i < captionPreprocessors.size(); i++) {
            captionFilterConf.setClass("filter.captions.pre." + i, captionPreprocessors.get(i), Preprocessor.class);
        }

        List<Class<? extends CaptionFilterHeuristic>> captionFilters = corpus.getCaptionFilterHeuristics();

        captionFilterConf.setInt("filter.captions.count", captionFilters.size());

        for (int i = 0; i < captionFilters.size(); i++) {
            captionFilterConf.setClass("filter.captions." + i, captionFilters.get(i), CaptionFilterHeuristic.class);
        }

        return captionFilterConf;
    }

    private Configuration getImageMatchingConf() {
        final Configuration imageMatchingConf = new Configuration(false);

        imageMatchingConf.setClass("matching.equality_metric", corpus.getImageEqualityClass(), EqualityMetric.class);

        return imageMatchingConf;
    }

    private Configuration getParaphraseFilterConf() {
        final Configuration paraphraseFilterConf = new Configuration(false);

        final List<Class<? extends ParaphraseFilterHeuristic>> paraphraseFilter = corpus.getParaphraseFilterHeuristics();
        paraphraseFilterConf.setInt("filter.stage.start", 2 + corpus.getImageFilterHeuristics().size() + corpus.getCaptionFilterHeuristics().size());
        paraphraseFilterConf.setInt("filter.paraphrases.count", paraphraseFilter.size());

        for (int i = 0; i < paraphraseFilter.size(); i++) {
            paraphraseFilterConf.setClass("filter.paraphrases." + i, paraphraseFilter.get(i), ParaphraseFilterHeuristic.class);
        }

        return paraphraseFilterConf;
    }

    private Configuration getParaphraseConstructionConf() {
        final Configuration paraphraseConstructionConf = new Configuration(false);

        paraphraseConstructionConf.setInt("filter.stage.start", 1 + corpus.getImageFilterHeuristics().size() + corpus.getCaptionFilterHeuristics().size());

        return paraphraseConstructionConf;
    }
}
