package edu.cu.boulder.cs.flink.triangles;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;

/**
 * This creates netflows from a pool of IPs.
 */
public class ModifiedNetflowSource extends RichParallelSourceFunction<SimplifiedNetflow> {

  public String inputFileName;

  public ModifiedNetflowSource(String inputDataFile)
  {
    this.inputFileName = inputDataFile;
  }

  @Override
  public void run(SourceContext<SimplifiedNetflow> out) throws Exception
  {
    RuntimeContext context = getRuntimeContext();
    int taskId = context.getIndexOfThisSubtask();

    try (BufferedReader br = new BufferedReader(new FileReader(inputFileName))) {
      String line;
      br.readLine();
      while ((line = br.readLine()) != null) {
        String[] values = line.split(",");
        long timeSeconds =Long.parseLong(values[0]);
        SimplifiedNetflow netflow = new SimplifiedNetflow(timeSeconds,values[10],values[11]);
        out.collectWithTimestamp(netflow, timeSeconds);
        out.emitWatermark(new Watermark(timeSeconds));
      }
    }



  }

  @Override
  public void cancel()
  {

  }
}
