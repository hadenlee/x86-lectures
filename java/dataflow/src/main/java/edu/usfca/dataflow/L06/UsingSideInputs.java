package edu.usfca.dataflow.L06;

import edu.usfca.dataflow.Main;
import edu.usfca.protobuf.L06.L06DeviceId;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.KvSwap;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UsingSideInputs {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    LOG.info("hello?! This is for L06!");
    // NOTE: This should end in ".../java/dataflow"
    final String rootDir = System.getProperty("user.dir");
    System.out.println("User Dir Root: " + rootDir);

    // ------------------------------------------------------
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    // ------------------------------------------------------
    // Let's begin with a dummy PC with App-DeviceId pairs.
    // An app is 'popular' if 3 or more distinct devices have it.
    // Note that 'app1', 'app2', and 'app3' are found in 3 distinct device IDs (each), and they are popular.
    // 'dev2' has all three popular apps, and 'dev3' and 'dev4' have two popular apps each.
    // Hence, we want to output 'dev2', 'dev3', and 'dev4' as final output.

    // -----------------------------------------------------------------
    // Notice that we're applying Distinct PTransform to begin with, in order to remove duplicates.
    PCollection<KV<String, L06DeviceId>> input = p.apply(Create.of(//
      KV.of("app1", L06DeviceId.newBuilder().setId("dev1").build()),//
      KV.of("app1", L06DeviceId.newBuilder().setId("dev1").build()),// <- same as above (duplicate)
      KV.of("app1", L06DeviceId.newBuilder().setId("dev2").build()),//
      KV.of("app1", L06DeviceId.newBuilder().setId("dev2").build()),// <- same as above (duplicate)
      KV.of("app1", L06DeviceId.newBuilder().setId("dev3").build()),//
      //
      KV.of("app2", L06DeviceId.newBuilder().setId("dev2").build()),//
      KV.of("app2", L06DeviceId.newBuilder().setId("dev3").build()),//
      KV.of("app2", L06DeviceId.newBuilder().setId("dev4").build()),//
      //
      KV.of("app3", L06DeviceId.newBuilder().setId("dev2").build()),//
      KV.of("app3", L06DeviceId.newBuilder().setId("dev4").build()),//
      KV.of("app3", L06DeviceId.newBuilder().setId("dev5").build()),//
      //
      KV.of("app4", L06DeviceId.newBuilder().setId("dev1").build()),//
      KV.of("app4", L06DeviceId.newBuilder().setId("dev5").build())//
    )).apply(Distinct.create()); // <- this will filter out two duplicate KVs.

    // We can apply GBK and DoFn here (you can do so as an exercise),
    // but let's utilize Count.perKey to count the number of unique device IDs per key.
    // This yields what we want because we've already applied 'Distinct' earlier.
    // Then, we'll apply Filter.by to only keep popular apps (here, threshold being 3 or more unique devices).
    // Lastly, we only need Apps (Strings) not KV<String, Long> (the values being counts), so we'd apply Keys.
    PCollection<String> popularApps = //
      input.apply(Count.perKey())//
        .apply(Filter.by(new SimpleFunction<KV<String, Long>, Boolean>() {
          @Override public Boolean apply(KV<String, Long> input) {
            return input.getValue() >= 3;
          }
        })).apply(Keys.create());

    // Now, we turn to 'Devices'. We first want to group Apps (as value) by DeviceId (as key).
    // Simplest method would be to apply KvSwap to input, and apply GroupByKey.
    // This yields KV<DeviceId, Iterable<String>> where Iterable<String> is a list of apps.
    PCollection<KV<L06DeviceId, Iterable<String>>> device2Apps =
      input.apply(KvSwap.create()).apply(GroupByKey.create());

    // Finally, we can use PCollectionView!
    // Let's turn 'popularApps' into a PCV.
    // Note that, conceptually, this PCollectionView contains a single List (of Strings).
    PCollectionView<List<String>> popularAppsView = popularApps.apply(View.asList());

    PCollection<L06DeviceId> devicesWithManyPopularApps =
      device2Apps.apply(ParDo.of(new DoFn<KV<L06DeviceId, Iterable<String>>, L06DeviceId>() {
        @ProcessElement public void process(ProcessContext c) {
          // We can access the contents of PCV (in this case, List<String>) by calling ProcessContext.sideInput().
          // The method requires a PCV reference.
          List<String> popularApps = c.sideInput(popularAppsView);
          int cntPopularApps = 0;
          // Now, we simply iterate over apps this device has, and counts how many of them are popular.
          // If the device has 2 or more popular apps, we'll output DeviceId.
          for (String app : c.element().getValue()) {
            if (!popularApps.contains(app)) {
              continue;
            }
            cntPopularApps += 1;
            if (cntPopularApps >= 2) {
              c.output(c.element().getKey());
              // You should expect to see something like the following on the console (order may be different):
              // DeviceId dev3 has 2 or more popular apps!
              // DeviceId dev2 has 2 or more popular apps!
              // DeviceId dev4 has 2 or more popular apps!
              System.out.format("DeviceId %s has 2 or more popular apps!\n", c.element().getKey().getId());
              return;
            }
          }
        }
      }).withSideInputs(popularAppsView));

    p.run().waitUntilFinish();
  }
}
