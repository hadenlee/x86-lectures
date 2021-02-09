package edu.usfca.dataflow.TeamQuiz02;

import edu.usfca.dataflow.TeamQuiz02.Problem2.MyDoFn2;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;

public class TestProblem2 implements Serializable {

  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Before public void before() {
    tp.getOptions().setStableUniqueNames(CheckEnabled.OFF);
  }

  @Test public void test2() {
    MyDoFn2 myDoFn = new MyDoFn2(10);

    PCollection<KV<String, Integer>> input = tp.apply(Create.of(//
      KV.of("A", 1),//
      KV.of("B", 11),//
      KV.of("C", 21)));

    PAssert.that(input.apply(ParDo.of(myDoFn))).containsInAnyOrder("A", "B", "C");

    tp.run();
  }
}
