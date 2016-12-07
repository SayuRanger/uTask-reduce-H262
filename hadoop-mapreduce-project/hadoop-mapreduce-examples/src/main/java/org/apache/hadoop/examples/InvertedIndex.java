package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

/**
 * Created by Dachuan Huang on 9/18/2016.
 */
public class InvertedIndex extends Configured implements Tool {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text filename = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String filenameStr = ((FileSplit)context.getInputSplit()).getPath().getName();
            filename = new Text(filenameStr);
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, filename);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            Set<String> set = new HashSet<String>();
            for (Text value : values) {
                set.add(value.toString());
            }
            StringBuffer sb = new StringBuffer();
            List<String> list = new ArrayList<String>(set);
            Collections.sort(list);
            for (String docname : list) {
                sb.append(String.format("%s ", docname));
            }
            context.write(key, new Text(sb.toString()));
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<String>();
            for (Text value : values) {
                set.add(value.toString());
            }
            for (String docname : set) {
                context.write(key, new Text(docname));
            }
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf());
        job.setJobName("invertedindex");
        job.setJarByClass(InvertedIndex.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < args.length; i++) {
            if ("-m".equals(args[i])) {
                throw new RuntimeException("-m not supported");
            } else if ("-r".equals(args[i])) {
                job.setNumReduceTasks(Integer.parseInt(args[++i]));
            } else {
                otherArgs.add(args[i]);
            }
        }

        Path inputPath = new Path(otherArgs.get(0));
        Path outputPath = new Path(otherArgs.get(1));

        // FileInputFormat.setInputDirRecursive(job, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        long t1 = System.currentTimeMillis();
        int ret = job.waitForCompletion(true) ? 0 : 1;
        long t2 = System.currentTimeMillis();
        System.out.println("job takes " + ((t2 - t1) / 1000.0) + " seconds");
        return ret;
    }

    public static void main(String[] args) throws Exception {
        InvertedIndex invertedIndex = new InvertedIndex();
        System.exit(ToolRunner.run(invertedIndex, args));
    }
}
