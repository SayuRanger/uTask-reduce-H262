package org.apache.hadoop.examples;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Dachuan Huang on 9/19/2016.
 */
public class SelfJoin extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(SelfJoin.class);

    public static class SelfJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int index = line.lastIndexOf(',');
            if (index == -1) {
                LOG.info("One invalid record " + line);
                return;
            }
            String kMinus1 = line.substring(0, index);
            String kth = line.substring(index + 1);
            context.write(new Text(kMinus1), new Text(kth));
        }
    }

    public static class SelfJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<String>();
            for (Text value : values) {
                set.add(value.toString());
            }
            List<String> kthlist = new ArrayList<String>(set);
            Collections.sort(kthlist);
            for (int i = 0; i < kthlist.size() - 1; i++) {
                for (int j = i + 1; j < kthlist.size(); j++) {
                    context.write(key, new Text(kthlist.get(i) + "," + kthlist.get(j)));
                }
            }
        }
    }

    public static class SelfJoinCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<String>();
            for (Text value : values) {
                set.add(value.toString());
            }
            for (String kth : set) {
                context.write(key, new Text(kth));
            }
        }
    }

    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConf());
        job.setJobName("selfjoin");
        job.setJarByClass(SelfJoin.class);
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(SelfJoinMapper.class);
        job.setCombinerClass(SelfJoinCombiner.class);
        job.setReducerClass(SelfJoinReducer.class);
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
        SelfJoin selfjoin = new SelfJoin();
        System.exit(ToolRunner.run(selfjoin, args));
    }
}
