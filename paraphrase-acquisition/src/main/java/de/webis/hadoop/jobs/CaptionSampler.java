package de.webis.hadoop.jobs;

import de.webis.hadoop.conf.CorpusConfig;
import de.webis.hadoop.formats.writables.SecondarySortWritable;
import de.webis.hadoop.mapper.CaptionSamplingMapper;
import de.webis.hadoop.reducer.CaptionSampleReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class CaptionSampler extends HadoopJob {
    private final int samples;

    public CaptionSampler(CorpusConfig corpus, boolean local) {
        super(corpus, local);

        this.samples = 500;
    }

    @Override
    protected Job configureJob(String[] args) throws IOException {
        final JobConf jobConf = new JobConf();
        jobConf.set("num-samples", String.valueOf(samples));

        final Job job = Job.getInstance(jobConf);
        job.setJobName("caption-sampling-" + corpus.getName());
        job.setJarByClass(CaptionSampler.class);

        Path inPath = new Path(corpus.getBasePath(), "images");
        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, inPath);

        job.setMapperClass(CaptionSamplingMapper.class);
        job.setMapOutputKeyClass(SecondarySortWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(SecondarySortWritable.WritablePartitioner.class);
        job.setGroupingComparatorClass(SecondarySortWritable.GroupingComparator.class);
        job.setSortComparatorClass(SecondarySortWritable.SortComparator.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(CaptionSampleReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        Path outPath = new Path(corpus.getBasePath(), "caption-sample");
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outPath);

        return job;
    }
}

