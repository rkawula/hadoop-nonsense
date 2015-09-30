import java.io.IOException;
import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;

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
                String currentToken = tokenizer.nextToken();
                if (previousToken != "") {
                    WordPairWritable wpw = new WordPairWritable(new Text(previousToken), new Text(currentToken));
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

    public static class WordPairWritable implements Writable {

        Text[] words = new Text[2];

        public WordPairWritable() {
            words[0] = new Text();
            words[1] = new Text();
        }

        public WordPairWritable(Text first, Text second) {
            words[0] = first;
            words[1] = second;
        }

        public void write(DataOutput dataOutput) throws IOException {
            words[0].write(dataOutput);
            words[1].write(dataOutput);
        }
 
        public void readFields(DataInput dataInput) throws IOException {
            words[0].readFields(dataInput);
            words[1].readFields(dataInput);
        }

        public Text getFirstWord() {
            return words[0];
        }

        public Text getSecondWord() {
            return words[1];
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
