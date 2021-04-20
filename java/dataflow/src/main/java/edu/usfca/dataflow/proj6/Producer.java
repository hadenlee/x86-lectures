package edu.usfca.dataflow.proj6;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import edu.usfca.dataflow.ProtoUtils;
import edu.usfca.protobuf.proj.Proj6.DeviceId;
import edu.usfca.protobuf.proj.Proj6.DeviceId.Builder;
import edu.usfca.protobuf.proj.Proj6.OsType;
import edu.usfca.protobuf.proj.Proj6.SalesEvent;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

// NOTE - This may be used in project 6 for live testing.
// It is currently experimental, and the code may be changed later.
public class Producer {
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  // TODO: Make sure you change the following four to your own settings.
  public final static String GCP_PROJECT_ID = "cs686-sp21-x48"; // <- TODO
  public final static String GCS_BUCKET = "gs://usf-my-bucket-x48"; // <- TODO

  public final static String MAIN_TOPIC_ID = "cs686-proj6-main"; // Don't change this.

  public final static String REGION = "us-west1"; // <- do not change.



  public static void run(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
    options = setDefaultValues(options);

    Pipeline p = Pipeline.create(options);

    if (options.getQps() <= 0 || options.getDuration() <= 0) {
      throw new IllegalArgumentException("--qps and --duration must be positive.");
    }


    PCollection<KV<Integer, Integer>> initData = p.apply(Create.of(KV.of(options.getDuration(), options.getQps())));
    ;

    if (options.getFullScale() && !options.getIsLocal()) {
      options.setMaxNumWorkers(1);
      if (options.getQps() >= 300) { // 300
        options.setWorkerMachineType("n1-standard-2");
        initData = p.apply(Create.of(KV.of(options.getDuration(), options.getQps() / 2)));
      } else {// 50, 150
        options.setWorkerMachineType("n1-standard-1");
      }

      initData = initData.apply(ParDo.of(new DoFn<KV<Integer, Integer>, KV<Integer, KV<Integer, Integer>>>() {
        @ProcessElement public void process(ProcessContext c) {
          for (int i = 0; i < 2_000_000; i++) {
            c.output(KV.of(i, c.element()));
          }
        }
      }))
        // -- Random shuffle to break fusion. Remember the quiz question?
        .apply(GroupByKey.create()).apply(Values.create()).apply(Flatten.iterables())
      // -- end of random shuffle.
      ;
    }

    initData.apply("PUBLISH", ParDo.of(new DoFn<KV<Integer, Integer>, String>() {
      List<String> data = new ArrayList<>();
      TopicName topicName;
      Publisher publisher;
      ByteString dummy;
      int cntPub = 0;
      boolean done = false;

      @Teardown public void tear() throws InterruptedException {
        if (publisher != null) {
          publisher.shutdown();
          publisher.awaitTermination(1, TimeUnit.MINUTES);
        }
      }

      ApiFutureCallback<String> callback;

      @Setup public void setup() throws IOException {
        callback = new ApiFutureCallback<String>() {
          public void onSuccess(String messageId) {
          }

          public void onFailure(Throwable t) {
          }
        };

        dummy = ByteString.copyFromUtf8(ProtoUtils.encodeMessageBase64(
          SalesEvent.newBuilder().setId(DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setWebid("dummy").build())
            .setBundle("dummy").setAmount(1).build()));

        topicName = TopicName.of(GCP_PROJECT_ID, MAIN_TOPIC_ID);
        publisher = Publisher.newBuilder(topicName)//
          .setBatchingSettings(
            BatchingSettings.newBuilder().setElementCountThreshold(10L).setDelayThreshold(Duration.ofMillis(10L))
              .setRequestByteThreshold(20_000L).build()).build();

        Random rnd = new Random(686);
        List<DeviceId> id = new ArrayList<>();
        List<String> bundle = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
          Builder did = DeviceId.newBuilder();
          did.setOs(OsType.forNumber(i % 2 + 1));
          if (i % 7 < 4) {
            did.setWebid("I." + getRandomString(rnd, 30));
          } else {
            did.setUuid(String.format("a000%04d-%04d-%04d-8000-b0120012ff15",//
              rnd.nextInt(10000), rnd.nextInt(10000), rnd.nextInt(10000)));
          }
          id.add(did.build());
        }
        for (int j = 0; j < 500; j++) {
          bundle.add("A." + getRandomString(rnd, 3));
        }
        SalesEvent.Builder se = SalesEvent.newBuilder();
        for (int i = 0; i < 50_000; i++) {
          se.setId(id.get(rnd.nextInt(id.size())));
          se.setBundle(bundle.get(rnd.nextInt(bundle.size())));
          se.setAmount(10_000 + rnd.nextInt(20));
          data.add(ProtoUtils.encodeMessageBase64(se.build()));
        }
      }

      void pub(ByteString data) {
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
        cntPub++;
        //          String msgId = publisher.publish(pubsubMessage).get();
        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        ApiFutures.addCallback(messageIdFuture, callback, MoreExecutors.directExecutor());
        if (cntPub % 200 == 1) {
          LOG.info("[pub] message cnt: {} (attempted) ", cntPub);
        }
      }

      void fireAndWait(long millis) {
        if (millis < 1)
          return;

        LOG.info("[fire dummy]");
        pub(dummy);

        try {
          Thread.sleep(millis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      @FinishBundle public void finish() {
        dummy = ByteString.copyFromUtf8(ProtoUtils.encodeMessageBase64(
          SalesEvent.newBuilder().setId(DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setWebid("dummy").build())
            .setBundle("dummy_end").setAmount(1).build()));
        final long t = Instant.now().getMillis();
        while ((Instant.now().getMillis() - t) / 1000L <= 30) {
          fireAndWait(100L);
        }
      }

      @ProcessElement public void process(ProcessContext c) {
        if (done) {
          return;
        }
        done = true;
        final long durationSec = c.element().getKey() * 60L;
        final long qps = c.element().getValue();
        final int numBatches = 200;

        long total = 0;
        int cntWindows = 0;

        final long t1 = Instant.now().getMillis();
        LOG.info("[process] starting up.. at t1 = {} ({})", t1, Instant.ofEpochMilli(t1));

        while ((Instant.now().getMillis() - t1) / 1000L / 60L <= 3) { // this part runs for up to 3 minutes.
          LOG.info("[waiting period] waiting... {}", Instant.now());
          while (true) {
            final long t = Instant.now().getMillis();
            long sec = (t / 1000L) % durationSec;
            if (sec == 0)
              break;
            else {
              fireAndWait(500L);
            }
          }
          cntWindows++;

          final long t2 = Instant.now().getMillis();
          LOG.info("[process] (windows {}) t2 = {} ({})", cntWindows, t2, Instant.ofEpochMilli(t2));

          int cnt = 0;
          Random rnd = new Random(68686 + ((t2 / 1000L / 60L) % 60));
          while (durationSec - (Instant.now().getMillis() / 1000L) % durationSec > 3) {
            if (cnt == numBatches * qps) {
              if (t2 + 59 * 1000L - Instant.now().getMillis() > 1000L) {
                fireAndWait(1000L);
                c.output(String.format("wait,%d,%d,%d", t1, t2, cnt));
              } else {
                fireAndWait(200L);
              }
              LOG.info("[process] waiting to close the window (cnt = {} with {} batches)", cnt, cnt / qps);
              continue;
            }
            final long begin = Instant.now().getMillis();
            for (long i = 0; i < qps; i++) {
              StringBuffer sb = new StringBuffer();
              for (int j = 0; j < 70; j++) {
                if (j == 0)
                  sb.append(data.get(rnd.nextInt(data.size())));
                else
                  sb.append(" " + data.get(rnd.nextInt(data.size())));
              }
              pub(ByteString.copyFromUtf8(sb.toString()));
              cnt++;
            }
            final long latency = Instant.now().getMillis() - begin;
            LOG.info("[process] moving on... cnt = {} ({}/{} check; latency {})", cnt, cnt / qps, numBatches, latency);
            fireAndWait(begin + 100 - Instant.now().getMillis());
            if (cnt == numBatches * qps) {
              LOG.info("[process] end for this window. cnt = {} check", cnt);
            }
          }
          total += cnt;
          LOG.info("[process] published {} messages ({} batches) (grand total {})", cnt, cnt / qps, total);
          c.output(String.format("closed,%d,%d,%d", t1, t2, cnt));
        }
        LOG.info("[process] published {} messages (grand total)", total);
      }

    })).

      apply(TextIO.write().to(GCS_BUCKET + "/proj6/dummy-output"));

    p.run().waitUntilFinish();
  }

  static String getRandomString(Random rnd, int len) {
    StringBuffer sb = new StringBuffer();
    // 33-126, inclusive.
    char x = (char) (33 + rnd.nextInt(94));
    sb.append(x);
    while (sb.length() < len - 1) {
      x = (char) (32 + rnd.nextInt(94));
      sb.append(x);
    }
    x = (char) (33 + rnd.nextInt(94));
    sb.append(x);
    return sb.toString();
  }

  static MyOptions setDefaultValues(MyOptions options) {
    System.out.format("user.dir: %s", System.getProperty("user.dir"));

    options.setJobName("proj6-producer");

    options.setTempLocation(GCS_BUCKET + "/staging2");
    if (options.getIsLocal()) {
      options.setRunner(DirectRunner.class);
    } else {
      options.setRunner(DataflowRunner.class);
    }
    if (options.getMaxNumWorkers() == 0) {
      options.setMaxNumWorkers(1);
    }
    if (StringUtils.isBlank(options.getWorkerMachineType())) {
      options.setWorkerMachineType("n1-standard-1");
    }
    options.setDiskSizeGb(150);
    options.setRegion(REGION);
    options.setProject(GCP_PROJECT_ID);

    // You will see more info here.
    // To run a pipeline (job) on GCP via Dataflow, you need to specify a few things like the ones above.
    LOG.info(options.toString());

    return options;
  }

}
