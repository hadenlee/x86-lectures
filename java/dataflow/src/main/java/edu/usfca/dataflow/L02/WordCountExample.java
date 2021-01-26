package edu.usfca.dataflow.L02;

import edu.usfca.dataflow.Main;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class WordCountExample {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! This is for L02!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // $line describes a PC of Strings where each string is a line from the input file.
    // TextIO assumes that input files are newline-delimited (by default).
    PCollection<String> lines =
      p.apply("read from files", TextIO.read().from(String.format("%s/resources/L02-sample.txt", rootDir)));

    // Using 'FlatMapElements' PTransform, we can turn each String (in $line) into an iterable of Strings (via 'split'),
    // which are 'flattened' subsequently so that we end up with PC<String> instead of PC<Iterable<String>>.
    // In other words, FlatMapElements helps us write concise code by applying 'flatten' behind the scenes (see below).
    PCollection<String> words = lines.apply(
      FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))));
    // The code below does the same as above, but it uses two "apply()" steps.
    //    PCollection<String> words = lines.apply(MapElements.into(TypeDescriptors.iterables(TypeDescriptors.strings()))
    //      .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+")))).apply(Flatten.iterables());

    // 'Count.perElement()' can be implemented in many different ways using other basic PTransforms (we'll revisit later).
    PCollection<KV<String, Long>> wordCount = words.apply(Count.perElement());

    // Here, we are chaining two PTransforms back-to-back.
    // The first one (Filter.by) keeps only the words that appear at least 100 times in the input file.
    // The second one (Keys.create()) drops the "values" in KV<?, ?>, thereby only keeping the keys (Strings in this case).
    PCollection<String> frequentWords =
      wordCount.apply(Filter.by((KV<String, Long> wordCountPair) -> (wordCountPair.getValue() >= 100)))
        .apply(Keys.create());

    // The pipeline will output the results to a file. Here, I'm specifically writing to a single 'txt' file.
    frequentWords
      .apply(TextIO.write().to(String.format("%s/resources/L02-output", rootDir)).withNumShards(1).withSuffix(".txt"));

    // Since we specified 'DirectRunner.class' above, this 'Pipeline (p)' will execute within the JVM
    // that runs this Java program. If we later run this pipeline using 'DataflowRunner.class' instead,
    // your JVM (local machine) will make gRPC calls to Google Cloud, and the job itself will execute there.
    // In that scenario, your JVM (local machine)'s job is to compile & build objects (PCollections and PTransforms),
    // and then to send those 'objects' in serialized format to the Google Cloud servers. We'll revisit this later.
    p.run().waitUntilFinish();
  }
}
