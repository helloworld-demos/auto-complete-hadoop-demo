package iam.demos;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

import static iam.demos.Constant.NUMBER_OF_GRAM;

public class NGramLibraryBuilder {
    private static final Logger logger = Logger.getLogger(AutoCompleteDriver.class);


    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void setup(Context context) {
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value is a sentence
            // what comes around goes around
            // for example, 3 gram:
            // what comes, comes around, around goes, goes around
            // what comes around, comes around goes, around goes around

            String sentence = value.toString()
                                   .toLowerCase();

            // remove useless characters (it is possible that it begins with digits, so remove beginning and tailing spaces in the end)
            sentence = sentence.replaceAll("[^a-z]", " ").trim();

            String[] allWords = sentence.split("\\s+");

            if (allWords.length <= 1) {
                // at least two words
                return;
            }

            StringBuilder sb;

            int noGram = context.getConfiguration().getInt(NUMBER_OF_GRAM, 5);

            for (int i = 0; i <= allWords.length - 2; i++) {
                // at least two words
                sb = new StringBuilder();

                sb.append(allWords[i]);

                // build library
                // what
                //     + comes
                //     + comes around
                //     + comes around goes
                //     + comes around goes around
                for (int j = 1; j <= noGram - 1 && i + j <= allWords.length - 1; j++) {
                    sb.append(" ")
                      .append(allWords[i + j]);

                    context.write(new Text(sb.toString()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // collect all the same grams
            int sum = 0;

            for (IntWritable value : values) {
                sum += value.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }
}