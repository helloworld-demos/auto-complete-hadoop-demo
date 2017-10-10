package iam.demos;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import static iam.demos.Constant.DELIMITER;

public class LanguageModel {
    private static final Logger logger = Logger.getLogger(LanguageModel.class);

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void setup(Context context) {
            // how to get the threashold parameter from the configuration?
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Input:  this is an example -> 10
            // Output: this is an -> example=10
            // NOT this is -> an example=10 TODO why?
            String[] pair = value.toString().split("\t");

            Integer count = Integer.valueOf(pair[1]);

            if (count < context.getConfiguration().getInt(Constant.THRESHOLD, 20)) {
                if (logger.isDebugEnabled()) {
                    logger.debug(String.format("%s is ignore since it only appears %d times.", pair[0], count));
                }
                return;
            }

            String sentence = pair[0];

            if (sentence.startsWith(" ") || sentence.endsWith(" ")) {
                logger.warn(sentence + " has empty beginning or tailing spaces");
                return;
            }

            if (sentence.split("\\s{2,}").length >= 2) {
                logger.warn(sentence + " has more than 2 spaces between words.");
                return;
            }

            int indexOfLastSpace = sentence.lastIndexOf(" ");

            String outputKey = sentence.substring(0, indexOfLastSpace);
            String outputValue = sentence.substring(indexOfLastSpace + 1) + DELIMITER + count;

            context.write(new Text(outputKey), new Text(outputValue));
        }
    }

    public static void main(String[] args) {
        System.out.println("a b c".split("\\s{2,}")[0]);
    }

    public static class Reduce extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        @Override
        public void setup(Context context) {
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Input Key:    this is an
            // Input Values: example=10 | outrage=2
            // Output: write {this is | example | 10 } into the database

            java.util.Map<Integer, List<String>> countToWords = new TreeMap<>(Collections.reverseOrder());

            for (Text value : values) {
                String[] wordAndCount = value.toString().split(DELIMITER);

                int count = Integer.parseInt(wordAndCount[1]);
                final String word = wordAndCount[0];

                if (countToWords.containsKey(count)) {
                    countToWords.get(count).add(word);
                } else {
                    countToWords.put(count, new ArrayList<String>() {{
                        add(word);
                    }});
                }
            }

            int topK = context.getConfiguration().getInt(Constant.TOP_K, 5);

            for (java.util.Map.Entry<Integer, List<String>> pair : countToWords.entrySet()) {
                if (topK <= 0) {
                    // only get top k result
                    break;
                }
                int count = pair.getKey();

                for (String word : pair.getValue()) {
                    context.write(new DBOutputWritable(key.toString(), word, count), NullWritable.get());
                    topK--;
                }
            }
        }
    }
}