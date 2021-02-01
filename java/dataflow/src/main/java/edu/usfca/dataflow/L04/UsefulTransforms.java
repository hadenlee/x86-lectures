package edu.usfca.dataflow.L04;

import com.google.common.collect.Iterables;
import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.KvSwap;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UsefulTransforms {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! This is for L04!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // ------------------------------------------------------
    // Let's create dummy PCs where keys are IDs (student IDs) and values are course numbers (in case of $courses)
    // or declared majors (in case of $majors).
    PCollection<KV<String, Integer>> courses =
      p.apply(Create.of(KV.of("id1", 486), KV.of("id1", 490), KV.of("id2", 686), KV.of("id2", 631), KV.of("id3", 686)));
    PCollection<KV<String, String>> majors =
      p.apply(Create.of(KV.of("id1", "CS"), KV.of("id2", "MATH"), KV.of("id4", "CS")));

    // Q1: What would $keys and $uniqueKeys contain in their PCollections?
    PCollection<String> keys = courses.apply(Keys.create());
    PCollection<String> uniqueKeys = keys.apply(Distinct.create());

    // Q2: What would $values and $uniqueValues contain in their PCollections?
    PCollection<Integer> values = courses.apply(Values.create());
    PCollection<Integer> uniqueValues = values.apply(Distinct.create());

    // Q3: What would $countKeys and $countValues contain in their PCollections?
    PCollection<KV<String, Long>> countKeys = courses.apply(Count.perKey());
    PCollection<KV<Integer, Long>> countValues = courses.apply(KvSwap.create()).apply(Count.perKey());
    // ------------------------------------------------------

    // Let's join courses and majors using their common key (Student ID).
    // We first create TupleTags as these 'tags' will be used to retrieve joined values.
    // Notice that we are first building a KeyedPCollectionTuple (by associating courses PC with courseTag and majors PC with majorTag).
    // As a result of CoGroupByKey, we get KV<String, CoGbkResult>.
    // CoGbkResult is a map from TupleTags to Iterables (hence, we need TupleTags!).
    TupleTag<Integer> courseTag = new TupleTag<>();
    TupleTag<String> majorTag = new TupleTag<>();
    PCollection<KV<String, CoGbkResult>> merged =
      KeyedPCollectionTuple.of(courseTag, courses).and(majorTag, majors).apply(CoGroupByKey.create());

    merged.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
      @ProcessElement public void process(@Element KV<String, CoGbkResult> elem, OutputReceiver<String> out) {
        final String key = elem.getKey();
        // When you check the messages printed to the console, you'll notice that CoGBK performs an outer-join (not inner-join).
        // Not familiar with inner-join vs. outer-join? Google it!
        Iterable<Integer> course = elem.getValue().getAll(courseTag);
        Iterable<String> major = elem.getValue().getAll(majorTag);
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
