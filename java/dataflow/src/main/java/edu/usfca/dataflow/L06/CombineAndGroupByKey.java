package edu.usfca.dataflow.L06;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.Main;
import edu.usfca.dataflow.ProtoUtils;
import edu.usfca.protobuf.L06.L06DeviceId;
import edu.usfca.protobuf.L06.L06Purchase;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class CombineAndGroupByKey {
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

    // ----------------------------------------------------------------------------
    // Let's implement a slightly complex aggregate function using GBK and Combine.
    // We'll work with a proto message that contains three fields (see L06.proto):
    // L06DeviceId dev_id = 1;
    // int64 iap_total = 2;
    // repeated string iap_app = 3;
    // We will simply sum up 'iap_total' for each device (IAP = in-app purchase) which hypothetically represents
    // the total amount of IAPs made by a device.
    // iap_app is a list of apps for which this device made at least one IAP.
    // In ProtocolBuffers, 'set<>' is not existent, so we need to ensure that we do not violate our own invariant
    // of keeping 'iap_app' distinct.
    // ----------------------------------------------------------------------------
    //
    // 1. Let's begin with some input data that's not yet 'structured' -- say, in CSV, where each record represents one IAP.
    // The data is formatted as: $device_id,$iap_amount,$app_id
    PCollection<String> input = p.apply(Create.of("dev1,100,app1", "dev1,200,app1", "dev1,150,app2", "dev1,486,app3", //
      "dev2,100,app2", "dev2,100,app2", "dev2,200,app3", "dev2,300,app3",//
      "dev3,100,app2", "dev3,150,app2", "dev3,686,app4", "dev3,150,app4"//
    ));

    // 2. Let's obtain more structured data using proto messages.
    PCollection<L06Purchase> purchaseData = input.apply(MapElements.via(new SimpleFunction<String, L06Purchase>() {
      @Override public L06Purchase apply(String input) {
        // Note that this code assumes that no input data is ill-formatted (otherwise, pipeline will fail).
        String[] tokens = input.split(",");
        return L06Purchase.newBuilder().setDevId(L06DeviceId.newBuilder().setId(tokens[0]).build()) //
          .setIapTotal(Long.parseLong(tokens[1])) //
          .addIapApp(tokens[2])//
          .build();
      }
    }));

    // 3. We'll apply GBK and DoFn to obtain desired output.
    // We first need to extract keys (using WithKeys.of).
    // We then apply GBK, and aggregate values using DoFn.
    PCollection<L06Purchase> outputGbk =
      purchaseData.apply(WithKeys.of(new SerializableFunction<L06Purchase, L06DeviceId>() {
        @Override public L06DeviceId apply(L06Purchase input) {
          return input.getDevId();
        }
      })).apply(GroupByKey.create()).apply(ParDo.of(new DoFn<KV<L06DeviceId, Iterable<L06Purchase>>, L06Purchase>() {
        @ProcessElement public void process(ProcessContext c) {
          final L06DeviceId key = c.element().getKey();
          // 'result' will be our final output for this device.
          L06Purchase.Builder result = L06Purchase.newBuilder();
          result.setDevId(key);

          // Iterate over value(s) to aggregate.
          for (L06Purchase purchase : c.element().getValue()) {
            // Simply sum up the total amount.
            result.setIapTotal(result.getIapTotal() + purchase.getIapTotal());
            // Simply append/add all apps (wait, what about duplicates?)
            result.addAllIapApp(purchase.getIapAppList());
          }
          // NOTE -- at this point, result may contain duplicate apps in its iap_app field (repeated string).
          // Let's de-duplicate by using a Set.
          Set<String> uniqueApps = new HashSet<>(result.getIapAppList());
          // Now, we'll clear (empty) result's iap_app, and add back distinct apps in the set.
          // We could have done better by starting with Set<String> from the beginning, but let's not worry about efficiency yet.
          result.clearIapApp();
          result.addAllIapApp(uniqueApps);

          c.output(result.build());
        }
      }));

    // 4. Now we'll use custom CombineFn to do the same. We will then compare the results.
    // As before, we first begin by extracting keys.
    // We then provide our custom CombineFn (using SerializableFunction).
    // Note that our SerializableFunction takes an Iterable of L06Purchase, and produces one L06Purchase message.
    // Beam pipeline will (under the hood) apply this per key (being L06DeviceId).
    PCollection<L06Purchase> outputCombine =
      purchaseData.apply(WithKeys.of(new SerializableFunction<L06Purchase, L06DeviceId>() {
        @Override public L06DeviceId apply(L06Purchase input) {
          return input.getDevId();
        }
      })).apply(Combine.perKey(new SerializableFunction<Iterable<L06Purchase>, L06Purchase>() {
        @Override public L06Purchase apply(Iterable<L06Purchase> input) {
          // Note that we have no access to 'key' within this method.
          // Technically, L06Purchase contains DeviceId, so we can use it, but let's pretend that we can't do that here.
          // The rest of the code here is almost identical to what we did in our custom DoFn (except for 'key').

          L06Purchase.Builder result = L06Purchase.newBuilder();
          for (L06Purchase purchase : input) {
            result.setIapTotal(result.getIapTotal() + purchase.getIapTotal());
            result.addAllIapApp(purchase.getIapAppList());
          }
          Set<String> uniqueApps = new HashSet<>(result.getIapAppList());
          result.clearIapApp();
          result.addAllIapApp(uniqueApps);

          return result.build();
        }
      })).apply(MapElements.via(new SimpleFunction<KV<L06DeviceId, L06Purchase>, L06Purchase>() {
        @Override public L06Purchase apply(KV<L06DeviceId, L06Purchase> input) {
          // Here, we are simply 'setting' the key (DeviceId) field in the final output.
          return input.getValue().toBuilder().setDevId(input.getKey()).build();
        }
      }));

    // 5. Let's compare two PCollections to confirm that they are identical.
    // One way to do that is to use CoGBK. We need keys, so let's extract keys again.
    // I'm chaining a lot of PTransforms here, so pay attention!
    TupleTag<L06Purchase> gbkTag = new TupleTag<>();
    TupleTag<L06Purchase> combineTag = new TupleTag<>();
    PCollection<KV<L06DeviceId, CoGbkResult>> outputAll =
      KeyedPCollectionTuple.of(gbkTag, outputGbk.apply(WithKeys.of(new SimpleFunction<L06Purchase, L06DeviceId>() {
        @Override public L06DeviceId apply(L06Purchase input) {
          return input.getDevId();
        }
      }))).and(combineTag, outputCombine.apply(WithKeys.of(new SimpleFunction<L06Purchase, L06DeviceId>() {
        @Override public L06DeviceId apply(L06Purchase input) {
          return input.getDevId();
        }
      }))).apply(CoGroupByKey.create());

    outputAll.apply(ParDo.of(new DoFn<KV<L06DeviceId, CoGbkResult>, Void>() {
      @ProcessElement public void process(ProcessContext c) throws InvalidProtocolBufferException {
        L06DeviceId key = c.element().getKey();
        // Notice that I'm using 'getOnly()' (instead of 'getAll()') here because I know that there should
        // only be one value per key (from each PCollection).
        // If that's not the case, something must have gone wrong, and getOnly() would throw an exception
        // that'll fail the pipeline.
        L06Purchase outputFromGbk = c.element().getValue().getOnly(gbkTag);
        L06Purchase outputFromCombine = c.element().getValue().getOnly(combineTag);

        // On your console, you should only see 'EQUAL' messages.
        // TODO - Technically, 'equals()' may not be the right way to compare these objects.
        // The reason being, proto's 'repeated' field is an ordered list, and thus even if the contents are the same,
        // the order may be different between these two objects. In this particular example, that won't likely happen,
        // but it could. What would be a better way to compare these two objects (efficiently)?
        if (outputFromGbk.equals(outputFromCombine)) {
          System.out.format("EQUAL! [%s]\n\n", ProtoUtils.getJsonFromMessage(outputFromGbk, false));
        } else {
          System.out.format("OH NO! [%s] [%s]\n\n", ProtoUtils.getJsonFromMessage(outputFromGbk, false),
            ProtoUtils.getJsonFromMessage(outputFromCombine, false));
        }
      }
    }));

    p.run().
      waitUntilFinish();
  }
}
