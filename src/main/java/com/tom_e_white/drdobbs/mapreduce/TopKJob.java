package com.tom_e_white.drdobbs.mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopKJob extends Configured implements Tool {

  public static final String TOP_K_PROPERTY = "topk";

  @Override
  public int run(String[] args) throws Exception {
    Job job = new Job();
    job.setJarByClass(getClass());
    job.setJobName(getClass().getSimpleName());

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    String k = args[2];

    job.getConfiguration().setInt(TOP_K_PROPERTY, Integer.parseInt(k));

    job.setInputFormatClass(KeyValueTextInputFormat.class);

    job.setMapperClass(TopKMapper.class);
    job.setReducerClass(TopKReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new TopKJob(), args);
    System.exit(rc);
  }
}