package de.webis.hadoop.jobs;

import de.webis.hadoop.conf.CorpusConfig;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public abstract class HadoopJob extends Configured implements Tool {
    public static final String DEFAULT_CONF_DIR = "/opt/hadoop-2.8.5/etc/hadoop";

    protected final CorpusConfig corpus;

    private final boolean local;

    public HadoopJob(CorpusConfig corpus, boolean local) {
        this.corpus = corpus;
        this.local = local;
    }

    protected abstract Job configureJob(String[] args) throws IOException;

    @Override
    public int run(String[] args) throws Exception {
        Job job = configureJob(args);

        Path outputPath = FileOutputFormat.getOutputPath(job);
        FileSystem fileSystem = outputPath.getFileSystem(job.getConfiguration());
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public boolean isLocal() {
        return local;
    }
}
