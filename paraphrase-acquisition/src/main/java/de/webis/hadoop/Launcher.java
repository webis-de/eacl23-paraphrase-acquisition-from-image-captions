package de.webis.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import de.webis.hadoop.conf.CorpusConfig;
import de.webis.hadoop.jobs.*;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;
import java.util.Optional;


public class Launcher {
    private static List<CorpusConfig> CORPORA;

    static {
        ObjectMapper objectMapper = new ObjectMapper();
        YAMLFactory yamlFactory = new YAMLFactory();
        try {
            YAMLParser yamlParser = yamlFactory.createParser(Launcher.class.getResource("/corpora.yaml"));

            CORPORA = objectMapper.readValues(yamlParser, CorpusConfig.class).readAll();
        } catch (IOException e) {
            System.err.println("ERROR: Parsing \"corpora.yaml\" failed!");
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static CorpusConfig getCorpus(String option) throws ParseException {
        Optional<CorpusConfig> configOptional = CORPORA.stream().filter(c -> c.getName().equalsIgnoreCase(option)).findAny();
        if (configOptional.isPresent())
            return configOptional.get();

        throw new ParseException("Can't find corpus \"" + option + "\"");
    }

    private static HadoopJob getJob(String method, CorpusConfig corpus, boolean local) throws ParseException {
        switch (method.toLowerCase()) {
            case "extract":
                return new ImageExtractor(corpus, local);
            case "sample-captions":
                return new CaptionSampler(corpus, local);
            case "sample-paraphrases":
                return new ParaphraseSampler(corpus, local);
            case "paraphrase":
                return new ParaphraseAcquisitionJob(corpus, local);
            default:
                throw new ParseException("Unknown method " + method);
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option methodOption = new Option("m", "method", true,
                "Method (\"extract\", \"sample-captions\")");
        methodOption.setRequired(true);
        options.addOption(methodOption);

        Option corpusOption = new Option("i", "input", true,
                "Input corpus (\"Wikipedia\", \"ClueWeb09\", \"ClueWeb12\", \"CommonCrawl\", \"Internet-Archive\"");
        corpusOption.setRequired(true);
        options.addOption(corpusOption);

        Option dbHostOption = new Option("dbh", "db-host", true, "Host of image DB");
        dbHostOption.setRequired(true);
        options.addOption(dbHostOption);

        Option dbPortOption = new Option("dbhp", "db-port", true, "Port of image DB");
        dbHostOption.setRequired(true);
        options.addOption(dbPortOption);

        Option dbUserOption = new Option("dbu", "db_user", true, "User name of image DB");
        dbUserOption.setRequired(false);
        options.addOption(dbUserOption);

        Option dbPasswordOption = new Option("dbp", "db_password", true, "Password of image DB user");
        dbPasswordOption.setRequired(false);
        options.addOption(dbPasswordOption);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        CorpusConfig corpus;
        HadoopJob hadoopJob;
        boolean local = HadoopJob.DEFAULT_CONF_DIR.equals(System.getenv("HADOOP_CONF_DIR"));

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            formatter.printHelp("paraphrase-pipeline", options);
            System.exit(1);
        }

        corpus = getCorpus(cmd.getOptionValue("i"));
        hadoopJob = getJob(cmd.getOptionValue("m"), corpus, local);

        String[] jobArgs = new String[]{
                cmd.getOptionValue("dbh"),
                cmd.getOptionValue("dbhp")
        };

        int status = ToolRunner.run(hadoopJob, jobArgs);
        System.exit(status);
    }

}
