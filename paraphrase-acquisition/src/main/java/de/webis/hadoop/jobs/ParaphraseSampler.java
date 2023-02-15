package de.webis.hadoop.jobs;

import de.webis.hadoop.conf.CorpusConfig;
import de.webis.hadoop.formats.filter.PartFileFilter;
import de.webis.hadoop.formats.writables.ParaphraseWritable;
import de.webis.hadoop.formats.writables.SecondarySortWritable;
import de.webis.hadoop.mapper.ParaphraseSamplingMapper;
import de.webis.hadoop.reducer.ParaphraseSampleReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class ParaphraseSampler extends HadoopJob {
    private final int samples;

    public ParaphraseSampler(CorpusConfig corpus, boolean local) {
        super(corpus, local);

        this.samples = 1000;
    }

    @Override
    protected Job configureJob(String[] args) throws IOException {
        final JobConf jobConf = new JobConf();
        jobConf.set("num-samples", String.valueOf(samples));
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        final Job job = Job.getInstance(jobConf);
        job.setJobName("paraphrase-sampling-" + corpus.getName());
        job.setJarByClass(ParaphraseSampler.class);

        Path inPath = new Path(corpus.getBasePath(), "paraphrases");
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, inPath);
        FileInputFormat.setInputPathFilter(job, PartFileFilter.class);

        job.setMapperClass(ParaphraseSamplingMapper.class);
        job.setMapOutputKeyClass(SecondarySortWritable.class);
        job.setMapOutputValueClass(ParaphraseWritable.class);

        job.setPartitionerClass(SecondarySortWritable.WritablePartitioner.class);
        job.setGroupingComparatorClass(SecondarySortWritable.GroupingComparator.class);
        job.setSortComparatorClass(SecondarySortWritable.SortComparator.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(ParaphraseSampleReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ParaphraseWritable.class);

        Path outPath = new Path(corpus.getBasePath(), "paraphrases-sample");
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        return job;
    }
}

