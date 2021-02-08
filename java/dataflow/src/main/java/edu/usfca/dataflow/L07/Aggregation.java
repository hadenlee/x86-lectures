package edu.usfca.dataflow.L07;

import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Aggregation {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! let's begin!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // ----------------------------------------------------------------------------
    PCollection<KV<String, Long>> kv = p.apply(Create.of(//
      KV.of("a", 1L), KV.of("a", 11L), KV.of("a", 21L),//
      KV.of("b", 2L), KV.of("b", 12L), KV.of("b", 31L),//
      KV.of("c", 3L), KV.of("c", 13L), KV.of("c", 41L),//
      KV.of("d", 4L), KV.of("d", 14L), KV.of("d", 51L),//
      KV.of("e", 5L), KV.of("e", 15L), KV.of("e", 61L)));

    PCollection<KV<String, Long>> sum1 =
      kv.apply(Sum.longsPerKey()); // Yay, Beam SDK already offers this functionality!

    // Here is one way to implement "Sum" using CombineFn where I'm using 'Long' for AccumT.
    // Note that this is NOT the most efficient way of implementing Sum, as discussed in lecture.
    PCollection<KV<String, Long>> sum2 = kv.apply(Combine.perKey(new CombineFn<Long, Long, Long>() {
      @Override public Long createAccumulator() {
        return new Long(0);
      }

      @Override public Long addInput(Long mutableAccumulator, Long input) {
        return new Long(mutableAccumulator + input);
      }

      @Override public Long mergeAccumulators(Iterable<Long> accumulators) {
        long sum = 0;
        for (Long acc : accumulators)
          sum += acc;
        return new Long(sum);
      }

      @Override public Long extractOutput(Long accumulator) {
        return accumulator;
      }
    }));

    // Let's compare sum1 and sum2 using CoGBK.
    // Instead of using TupleTags, we can use "String" names for shorter code.
    // The downside of this approach is, unlike TupleTags, the compiler can't type-check at compile time.
    // Therefore, this code will throw a ClassCastException (which is a RuntimeException).
    // Here is the expected output (to the console): (Order may differ)
    // As expected, the sum (33) is equal for key a
    // As expected, the sum (45) is equal for key b
    // As expected, the sum (81) is equal for key e
    // As expected, the sum (69) is equal for key d
    // As expected, the sum (57) is equal for key c
    KeyedPCollectionTuple.of("sum1", sum1).and("sum2", sum2).apply(CoGroupByKey.create())
      .apply(ParDo.of(new CompareTwoSums()));

    p.run().
      waitUntilFinish();
  }

  public static class CompareTwoSums extends DoFn<KV<String, CoGbkResult>, Void> {
    @ProcessElement public void process(ProcessContext c) {
      final String key = c.element().getKey();
      // TODO - Fix this code before running it!
      // In the next two lines, compiler can't check if your proposed types are correct/safe or not.
      // You should fix these two lines. (TODO)
      final Integer s1 = c.element().getValue().getOnly("sum1", 0);
      final Integer s2 = c.element().getValue().getOnly("sum2", 0);
      // ---------------------------------------------------------------------------------------------
      if (s1.compareTo(s2) == 0) {
        System.out.format("As expected, the sum (%d) is equal for key %s\n", s1, key);
      } else {
        throw new RuntimeException(
          String.format("Something must have gone wrong for key %s (got %d vs %d)\n", key, s1, s2));
      }
    }
  }
}
