package edu.usfca.dataflow.L03;

import edu.usfca.dataflow.Main;
import edu.usfca.protobuf.L03.MyProduct;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This sample code accompanies Lecture L02.
 * <p>
 * Read the detailed comments to learn more about Beam SDK.
 */
public class RevenueExample {
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

    // $line describes a PC of Strings where each element is a line from the input file.
    // TextIO assumes that input files are newline-delimited (by default).
    PCollection<String> lines =
      p.apply("read from files", TextIO.read().from(String.format("%s/resources/L03-d*.txt", rootDir)));

    // -----------------------------------------------------------------------------
    // DoFn simply describes a custom function (data transformation)
    // that you wish to apply to data in parallel (hence, 'Parallel Do = ParDo').
    // You'll be using DoFns often in your assignments, in addition to Beam-provided PTransforms.
    // In this sample code, we're defining an anonymous DoFn class (for simplicity),
    // but it's often (if not always) better to refactor it out as a static class for easier testing.
    PCollection<MyProduct> products = lines.apply(ParDo.of(new DoFn<String, MyProduct>() {
      @ProcessElement public void process(@Element String elem, OutputReceiver<MyProduct> out) {
        // You'll notice that this prints to the console output in no particular order
        // (and it changes every time you run your Java program). This is because "PCollection" does not
        // preserve/maintain order at all (for the sake of maximum parallelization).
        System.out.format("Line read: %s\n", elem);
        //------------------------------------------------------------------

        out.output(getMyProduct(elem));
      }
    }));
    // ^ TODO #1 - as an exercise, try to use "MapElements" in Beam SDK in place of ParDo-DoFn to achieve the same result.
    // Here is one starting point:
    //    lines.apply(MapElements.into(TypeDescriptor.of( ... )).via(new ProcessFunction<String, ...>() {
    //      @Override public ... apply(String input) throws Exception {
    //        System.out.format("Line read: %s\n", input);
    //        return getMyProduct(input);
    //      }
    //    }));

    // -----------------------------------------------------------------------------
    // Before we aggregate (reduce) the values in 'MyProduct', we need to map it to KVs.
    // Here is one way to do so using DoFn:
    PCollection<KV<String, MyProduct>> keyedProducts =
      products.apply(ParDo.of(new DoFn<MyProduct, KV<String, MyProduct>>() {
        @ProcessElement public void process(@Element MyProduct elem, OutputReceiver<KV<String, MyProduct>> out) {
          out.output(KV.of(elem.getId(), elem));
        }
      }));
    // ^ TODO #2 - as an exercise, try to use one of the element-wise PTransforms provided by Beam SDK
    // that is designed exactly for this kind of operation. (Hint: the transform has to do with 'keys').

    // -----------------------------------------------------------------------------
    // Let's apply GroupByKey step that is one of the most useful PTransforms.
    // Then, we'll apply another DoFn that implements our custom 'aggregator'.
    // This is a common pattern in MapReduce: GroupByKey followed by Reduce (aggregate).
    PCollection<MyProduct> aggregatedProducts = keyedProducts.apply(GroupByKey.create()) //
      .apply(ParDo.of(new DoFn<KV<String, Iterable<MyProduct>>, MyProduct>() {
        @ProcessElement
        public void process(@Element KV<String, Iterable<MyProduct>> elem, OutputReceiver<MyProduct> out) {
          out.output(mergeMyProducts(elem));
        }
      }));

    // -----------------------------------------------------------------------------
    // Let's write to files. TextIO.write() only accepts 'PCollection<String>', so we need another Map step.
    aggregatedProducts.apply(MapElements.via(new SimpleFunction<MyProduct, String>() {
      @Override public String apply(MyProduct prod) {
        return String.format("%s,%d,%d", prod.getId(), prod.getUnitsSold(), prod.getRevenue());
      }
    })).apply(TextIO.write().to(String.format("%s/resources/L03-output", rootDir)).withSuffix(".txt").withNumShards(1));

    p.run().
      waitUntilFinish();
  }

  // TODO #3 - This method works OK for the input files provided.
  // However, if the Product ID contains a comma or some lines are invalid, then this approach may not work properly.
  // What would be a better way to parse CSV files?
  static MyProduct getMyProduct(String line) {
    // Assuming that the input data is 'valid', we can tokenize each line to three String literals.
    // These tokens correspond to product ID, units sold, and revenue.
    String[] tokens = line.split(",");
    MyProduct.Builder prodBuilder = MyProduct.newBuilder();

    // You can "set" the value of a field like this:
    prodBuilder.setId(tokens[0]);

    // You can chain multiple calls to setters, too.
    prodBuilder.setUnitsSold(Integer.parseInt(tokens[1])).setRevenue(Integer.parseInt(tokens[2]));

    // 'build()' will create a new object based on the values set in the builder object.
    return prodBuilder.build();
  }

  static MyProduct mergeMyProducts(KV<String, Iterable<MyProduct>> keyedProducts) {
    // Set the ID.
    MyProduct.Builder builder = MyProduct.newBuilder().setId(keyedProducts.getKey());

    // revenue and unitsSold should be 'summed up' (their default values are 0s in protobuf).
    for (MyProduct product : keyedProducts.getValue()) {
      builder.setRevenue(builder.getRevenue() + product.getRevenue());
      builder.setUnitsSold(builder.getUnitsSold() + product.getUnitsSold());
    }

    return builder.build();
  }
}
