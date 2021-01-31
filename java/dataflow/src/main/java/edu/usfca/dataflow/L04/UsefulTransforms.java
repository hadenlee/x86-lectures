package edu.usfca.dataflow.L04;

import com.google.common.collect.Iterables;
import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UsefulTransforms {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! This is for L03!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // ------------------------------------------------------
    PCollection<KV<String, Integer>> input =
      p.apply(Create.of(KV.of("id1", 486), KV.of("id1", 490), KV.of("id2", 686), KV.of("id2", 631), KV.of("id3", 686)));


    PCollection<String> keys = input.apply(Keys.create());
    PCollection<String> uniqueKeys = keys.apply(Distinct.create());


    PCollection<Integer> values = input.apply(Values.create());
    PCollection<Integer> uniqueValues = values.apply(Distinct.create());
    // ------------------------------------------------------
    PCollection<KV<String, String>> input2 =
      p.apply(Create.of(KV.of("id1", "CS"), KV.of("id2", "MATH"), KV.of("id4", "CS")));

    PCollection<KV<String, CoGbkResult>> merged =
      KeyedPCollectionTuple.of("courses", input).and("major", input2).apply(CoGroupByKey.create());
    merged.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
      @ProcessElement public void process(@Element KV<String, CoGbkResult> elem, OutputReceiver<String> out) {
        final String key = elem.getKey();
        Iterable<Integer> course = elem.getValue().getAll("courses");
        Iterable<String> major = elem.getValue().getAll("major");
        out.output(String.format("Student Id: %s\t courses: %s\t major: %s \n", key, Iterables.toString(course),
          Iterables.toString(major)));
      }
    })).apply(ParDo.of(new DoFn<String, Void>() {
      @ProcessElement public void process(@Element String elem) {
        System.out.println(elem);
      }
    }));
    
    p.run().
      waitUntilFinish();
  }
}
