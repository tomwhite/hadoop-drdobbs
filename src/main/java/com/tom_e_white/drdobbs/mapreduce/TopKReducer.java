package com.tom_e_white.drdobbs.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TopKReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  private TopKCollection topK;

  @Override
  protected void setup(Context context) {
    int k = context.getConfiguration().getInt(TopKJob.TOP_K_PROPERTY, 10);
    this.topK = new TopKCollection(k);
  }

  @Override
  protected void reduce(Text word, Iterable<LongWritable> counts, Context context) {
    for (LongWritable count : counts) {
      topK.add(word.toString(), count.get());
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    List<Set<String>> wordSets = topK.getWordSets();
    List<Long> counts = topK.getCounts();
    Text wordText = new Text();
    LongWritable count = new LongWritable();
    int recordsWritten = 0;
    for (int i = 0; i < wordSets.size(); i++) {
      count.set(counts.get(i));
      for (String word : wordSets.get(i)) {
        wordText.set(word);
        context.write(wordText, count);
        recordsWritten++;
      }
      if (recordsWritten >= topK.getK()) {
        return;
      }
    }
  }
}
