package com.tom_e_white.drdobbs.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.lib.Aggregate;
import org.apache.crunch.types.avro.Avros;
import org.apache.crunch.util.CrunchTool;
import org.apache.hadoop.util.ToolRunner;

import java.io.Serializable;

public class TopK extends CrunchTool implements Serializable {

  @Override
  public int run(String[] args) throws Exception {
    int k = Integer.parseInt(args[2]);
    PCollection<String> lines = readTextFile(args[0]);
    PTable<String, Long> records = lines
      .parallelDo(new DoFn<String, Pair<String, Long>>() {
        @Override
        public void process(String line, Emitter<Pair<String, Long>> emitter) {
          String[] split = line.split("\t+");
          String word = split[0];
          long count = Long.parseLong(split[2]);
          emitter.emit(Pair.of(word, count));
        }
      }, Avros.tableOf(Avros.strings(), Avros.longs()));
    PGroupedTable<String, Long> groupedRecords = records
      .groupByKey();
    PTable<String, Long> counts = groupedRecords
      .combineValues(Aggregators.SUM_LONGS());
    PTable<String, Long> topk = Aggregate.top(counts, k, true);
    writeTextFile(topk, args[1]);
    run();
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new TopK(), args);
    System.exit(rc);
  }
}
