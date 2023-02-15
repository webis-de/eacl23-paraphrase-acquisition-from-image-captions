package de.webis.hadoop.jobs;

import de.webis.hadoop.conf.CorpusConfig;
import de.webis.hadoop.formats.input.FilePathInputFormat;
import de.webis.hadoop.formats.input.WikiDumpInputFormat;
import de.webis.hadoop.formats.output.MongoDBOutputFormat;
import de.webis.hadoop.formats.writables.ImageReferenceWritable;
import de.webis.hadoop.formats.writables.ImageWritable;
import de.webis.hadoop.mapper.WarcImageExtractionMapper;
import de.webis.hadoop.mapper.WikiImageExtractionMapper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class ImageExtractor extends HadoopJob {
    public ImageExtractor(CorpusConfig corpus, boolean local) {
        super(corpus, local);
    }

    @Override
    protected Job configureJob(String[] args) throws IOException {
        Configuration jobConf = new Configuration();

        final long mapMemory = corpus.getImageExtractionMapperMemory();
        final long reduceMemory = corpus.getImageExtractionReducerMemory();

        jobConf.set("mapreduce.map.memory.mb", String.valueOf(mapMemory));
        jobConf.set("mapreduce.reduce.memory.mb", String.valueOf(reduceMemory));

        jobConf.set("mapreduce.map.java.opts", "-Xmx" + (int) (0.8 * mapMemory) + "m");
        jobConf.set("mapreduce.reduce.java.opts", "-Xmx" + (int) (0.8 * reduceMemory) + "m");

        jobConf.set("mongodb.host", args[0]);
        jobConf.setInt("mongodb.port", Integer.parseInt(args[1]));
        jobConf.set("mongodb.database", corpus.getName());

        final Job job = Job.getInstance(jobConf);
        job.setJobName("image-extraction-" + corpus.getName());
        job.setJarByClass(ImageExtractor.class);

        if (isLocal()) {
            for (String localPath : corpus.getLocalFilePaths()) {
                FileInputFormat.addInputPath(job, new Path(localPath));
            }
        } else {
            for (String remotePath : corpus.getRemoteFilePaths()) {
                FileInputFormat.addInputPath(job, new Path(remotePath));
            }
        }

        Class<? extends PathFilter> pathFilterClass = corpus.getFilePathFilter();
        if (pathFilterClass != null)
            FileInputFormat.setInputPathFilter(job, pathFilterClass);

        if (corpus.getFiletype().equals("warc")) {
            job.setInputFormatClass(FilePathInputFormat.class);
            job.setMapperClass(WarcImageExtractionMapper.class);
        } else if (corpus.getFiletype().equals("wikitext")) {
            job.setInputFormatClass(WikiDumpInputFormat.class);
            job.setMapperClass(WikiImageExtractionMapper.class);
        } else {
            throw new NotImplementedException("Unknown file type \"" + corpus.getFiletype() + "\"");
        }

        job.setMapOutputKeyClass(ImageWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(ImageWritable.class);
        job.setOutputValueClass(NullWritable.class);
        Path outPath = new Path(corpus.getBasePath(), "image-references");
        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(MongoDBOutputFormat.class);

        MultipleOutputs.addNamedOutput(job, "references", SequenceFileOutputFormat.class,
                ImageReferenceWritable.class, NullWritable.class);

        return job;
    }
}
