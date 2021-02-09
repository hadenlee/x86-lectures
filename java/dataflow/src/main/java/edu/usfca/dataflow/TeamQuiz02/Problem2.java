package edu.usfca.dataflow.TeamQuiz02;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class Problem2 {
  // TODO - Problem 2:
  // This DoFn is nearly identical to MyDoFn1 (but I've removed some lines of code that are irrelevant).
  // If you compare test1() and test2(), even though we are using the same input and same parameter (10),
  // the results are different (the output PCollection's contents).
  // Why is this happening? (The expected answer is short, like 2-3 sentences should be sufficient.)
  public static class MyDoFn2 extends DoFn<KV<String, Integer>, String> {
    transient int param;

    public MyDoFn2(int param) {
      this.param = param;
    }

    @ProcessElement public void process(ProcessContext c) {
      if (c.element().getValue() >= param) {
        c.output(c.element().getKey());
      }
    }
  }
}
