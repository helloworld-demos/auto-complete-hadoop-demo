package iam.demos;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static iam.demos.Constant.*;

public class AutoCompleteDriver {
    private static final Logger logger = Logger.getLogger(AutoCompleteDriver.class);

    private static final String NO_GRAM_KEY = "noGram";
    private static final String INPUT_PATH_KEY = "inputPath";
    private static final String TEMP_PATH_KEY = "tempPath";
    private static final String THRESHOLD_KEY = "threshold";
    private static final String TOP_K_KEY = "topK";
    private static final String MYSQL_CONNECTOR_PATH_KEY = "mysqlConnectorPath";

    // -DnoGram=3 -DinputPath=input -DtempPath=/output -Dthreshold=30 -DtopK=7 -DmysqlConnectorPath=
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Map<String, String> arguments = new HashMap<>();

        for (String arg : args) {
            if (arg.startsWith("-D")) {
                String[] pair = arg.replace("-D", "").split("=");
                arguments.put(pair[0], pair[1]);
                logger.info(String.format("Receive parameter pair %s:%s", pair[0], pair[1]));
            }
        }

        if (!arguments.containsKey(INPUT_PATH_KEY) || !arguments.containsKey(TEMP_PATH_KEY)) {
            throw new Error(String.format("%s and %s must exist.", INPUT_PATH_KEY, TEMP_PATH_KEY));
        }

        NGramLibraryBuilderJob(arguments.get(NO_GRAM_KEY),
                               arguments.get(INPUT_PATH_KEY),
                               arguments.get(TEMP_PATH_KEY))
                .waitForCompletion(true);

        LanguageModelJob(Integer.parseInt(arguments.get(THRESHOLD_KEY)),
                         arguments.get(TEMP_PATH_KEY),
                         Integer.parseInt(arguments.get(TOP_K_KEY)),
                         arguments.get(MYSQL_CONNECTOR_PATH_KEY))
                .waitForCompletion(true);
    }

    private static Job NGramLibraryBuilderJob(String noGram, String inputPath, String outputPath) throws IOException {
        Configuration config = new Configuration();

        // by default, hadoop mapper is reading the line from the file, but we need to build ngram based on a sentence
        config.set("textinputformat.record.delimiter", ".");
        config.set(NUMBER_OF_GRAM, noGram);

        Job instance = Job.getInstance(config);

        instance.setJobName(NGramLibraryBuilder.class.getName());
        instance.setJarByClass(AutoCompleteDriver.class);

        instance.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
        instance.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

        instance.setOutputKeyClass(Text.class);
        instance.setOutputValueClass(IntWritable.class);

        instance.setInputFormatClass(TextInputFormat.class);
        instance.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.setInputPaths(instance, new Path(inputPath));
        TextOutputFormat.setOutputPath(instance, new Path(outputPath));

        return instance;
    }

    private static Job LanguageModelJob(int threshold, String inputPath, int topK, String mysqlConnectorPath) throws IOException {
        Configuration config = new Configuration();

        config.setInt(THRESHOLD, threshold);
        config.setInt(TOP_K, topK);

        DBConfiguration.configureDB(config,
                                    DRIVER_CLASS,
                                    DB_URL,
                                    DB_USER_NAME,
                                    DB_PASSWORD);

        Job job = Job.getInstance(config);

        job.setJobName(LanguageModel.class.getName());
        job.setJarByClass(AutoCompleteDriver.class);

        job.setMapperClass(LanguageModel.Map.class);
        job.setReducerClass(LanguageModel.Reduce.class);

        // TODO what is better way to do it
        job.addArchiveToClassPath(new Path(mysqlConnectorPath));

        // TODO why
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(job, "auto_complete",
                                 "starting_phrase", "following_word", "count");

        TextInputFormat.setInputPaths(job, inputPath);

        return job;
    }
}