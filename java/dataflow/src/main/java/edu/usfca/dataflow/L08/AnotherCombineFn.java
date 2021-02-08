package edu.usfca.dataflow.L08;

import edu.usfca.dataflow.L07.Aggregation.CompareTwoSums;
import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AnotherCombineFn {
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

    // Here is another way to implement "Sum" using CombineFn where I'm using 'List<Long>' for AccumT.
    // Note that this is NOT the most efficient way of implementing Sum.
    // Notice that each accumulator (List<Long>) is lazily adding inputs to itself, until 'extract' is called.
    // TODO: If you run this code as-is, it will throw an exception in line marked with (*).
    // Figure out why that's the case, and fix it. You only need to fix one line (but not (*)).
    // Using a debugger may be helpful.
    PCollection<KV<String, Long>> sum2 = kv.apply(Combine.perKey(new CombineFn<Long, List<Long>, Long>() {
      @Override public List<Long> createAccumulator() {
        return Arrays.asList(0L);
      }

      @Override public List<Long> addInput(List<Long> mutableAccumulator, Long input) {
        mutableAccumulator.add(input); // (*) -- this throws an exception.
        return mutableAccumulator;
      }

      @Override public List<Long> mergeAccumulators(Iterable<List<Long>> accumulators) {
        Iterator<List<Long>> it = accumulators.iterator();
        List<Long> result = it.next();
        while (it.hasNext()) {
          result.addAll(it.next());
        }
        return result;
      }

      @Override public Long extractOutput(List<Long> accumulator) {
        long sum = 0;
        for (Long val : accumulator)
          sum += val;
        return sum;
      }
    }));

    // Let's compare sum1 and sum2 using CoGBK.
    // We're re-using the code from L07, so you should have updated that first.
    // Expected output is the same as L07 sample code.
    KeyedPCollectionTuple.of("sum1", sum1).and("sum2", sum2).apply(CoGroupByKey.create())
      .apply(ParDo.of(new CompareTwoSums()));

    p.run().waitUntilFinish();
  }
}
