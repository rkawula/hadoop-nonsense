import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordPairCount {

    public static class Map extends Mapper<LongWritable, Text, WordPairWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String previousToken = "";
            while (tokenizer.hasMoreTokens()) {
                String currentToken = tokenizer.nextToken());
                if (previousToken != "") {
                    WordPairWritable wpw = new WordPairWritable(previousToken, currentToken);
                    context.write(wpw, one);
                }
                previousToken = currentToken;
            }
        }
    }

    public static class Reduce extends Reducer<WordPairWritable, IntWritable, WordPairWritable, IntWritable> {

        public void reduce(WordPairWritable keyPair, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(keyPair, new IntWritable(sum));
        }
    }

    public static class WordPairWritable extends ArrayWritable {

        Text[] words = new Text[2];

        public WordPairWritable(Text first, Text second) {
            super(WordPairWritable.class);
            words[0] = first;
            words[1] = second;
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordpaircount");

        job.setOutputKeyClass(WordPairWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(WordPairCount.class);
        job.waitForCompletion(true);
    }

}
