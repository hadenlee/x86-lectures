package edu.usfca.dataflow.L05;

import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.ProcessFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MoreTransforms {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! This is for L05!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // ------------------------------------------------------
    // Let's begin with a dummy PC with strings.
    PCollection<String> letters = p.apply(Create.of("a", "b", "c", "d", "p", "q", "r", "x", "y", "z"));

    // Note: You can replace 'PartitionFn' by a lambda expression (your IDE may suggest it).
    PCollectionList<String> partitionedLetters = letters.apply(Partition.of(3, new PartitionFn<String>() {
      @Override public int partitionFor(String elem, int numPartitions) {
        // Use 0-based index.
        if (elem.charAt(0) <= 'e')
          return 1;
        if (elem.charAt(0) >= 'x')
          return 2;
        return 0;
      }
    }));

    // Since we partitioned one PC into 3 PCs, we can now work with 3 different PCs, applying different PTs.
    // Let's apply some silly PTransforms (I'm also demonstrating different ways to use 'MapElements' PTransform).
    PCollection<String> pc0 = partitionedLetters.get(0).apply(MapElements.via(new SimpleFunction<String, String>() {
      @Override public String apply(String input) {
        return "ONE:" + input;
      }
    }));

    // Below, SerializableFunction can be replaced by a lambda expression (your IDE may suggest so).
    PCollection<String> pc1 = partitionedLetters.get(1)
      .apply(MapElements.into(TypeDescriptors.strings()).via(new SerializableFunction<String, String>() {
        @Override public String apply(String input) {
          return "TWO:" + input;
        }
      }));

    // Below, ProcessFunction can be replaced by a lambda expression (your IDE may suggest so).
    PCollection<String> pc2 = partitionedLetters.get(2)
      .apply(MapElements.into(TypeDescriptors.strings()).via(new ProcessFunction<String, String>() {
        @Override public String apply(String input) throws Exception {
          if (StringUtils.isBlank(input)) {
            throw new RuntimeException("NO!!!");
          }
          return "THREE:" + input + input + input;
        }
      }));

    // Let's merge these three PCs into one using Flatten.
    // Then, let's filter elements so that we only keep the ones that contain 'a' or 'p' or 'x'.
    // Note that this ProcessFunction can be replaced by a lambda expression (your IDE may suggest so), too.
    PCollection<String> output = PCollectionList.of(Arrays.asList(pc0, pc1, pc2)).apply(Flatten.pCollections())
      .apply(Filter.by(new ProcessFunction<String, Boolean>() {
        @Override public Boolean apply(String input) throws Exception {
          return (StringUtils.containsAny(input, 'a', 'p', 'x'));
        }
      }));

    output.apply(ParDo.of(new DoFn<String, Void>() {
      // 'ProcessContext' is another way to implement your DoFn's main method.
      // it offers .element() (for retrieving input elements) and .output() (both for single output and multi output).
      // You can use either ProcessContext or annotated '@Element' and OutputReceiver (it's up to you).
      // ProcessContext was introduced at the very early stage of Beam SDK (so I'm more familiar with it),
      // but Beam's more 'modern' style is to use Java annotations more.
      @ProcessElement public void process(ProcessContext c) {
        // Expect to see (order may be different):
        // Flattened PCollection contains: THREE:xxx
        // Flattened PCollection contains: ONE:p
        // Flattened PCollection contains: TWO:a
        String elem = c.element();
        System.out.println("Flattened PCollection contains: " + elem);
      }
    }));

    // An example for CoGroupByKey can be found in L04 code (UsefulTransforms class).

    p.run().
      waitUntilFinish();
  }
}
