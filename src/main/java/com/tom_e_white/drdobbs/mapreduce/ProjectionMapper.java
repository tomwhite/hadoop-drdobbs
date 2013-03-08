package com.tom_e_white.drdobbs.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProjectionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private Text word = new Text();
  private LongWritable count = new LongWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // value is tab separated values: word, year, occurrences, #books, #pages
    // we project out (word, occurrences) so we can sum over all years
    String[] split = value.toString().split("\t+");
    word.set(split[0]);
    if (split.length > 2) {
      try {
        count.set(Long.parseLong(split[2]));
        context.write(word, count);
      } catch (NumberFormatException e) {
        // cannot parse - ignore
      }
    }
  }
}
