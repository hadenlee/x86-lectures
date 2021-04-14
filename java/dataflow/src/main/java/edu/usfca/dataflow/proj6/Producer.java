package edu.usfca.dataflow.proj6;

import com.google.cloud.pubsub.v1.Publisher;
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Producer {
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

  // TODO: Make sure you change the following four to your own settings.
  public final static String GCP_PROJECT_ID = "cs686-sp21-x48"; // <- TODO
  public final static String GCS_BUCKET = "gs://usf-my-bucket-x48"; // <- TODO

  public final static String MAIN_TOPIC_ID = "cs686-proj6-main"; // Don't change this.

  public final static String REGION = "us-west1"; // <- do not change.


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

  public static void run(String[] args) {
    MyOptions options = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
    options = setDefaultValues(options);

    Pipeline p = Pipeline.create(options);

    if (options.getQps() <= 0 || options.getDuration() <= 0) {
      throw new IllegalArgumentException("--qps and --duration must be positive.");
    }

    p.apply(Create.of(KV.of(options.getDuration(), options.getQps())))
      .apply(ParDo.of(new DoFn<KV<Integer, Integer>, String>() {
        List<String> data = new ArrayList<>();
        TopicName topicName;
        Publisher publisher;
        ByteString dummy;
        int cntPub = 0;

        @Teardown public void tear() throws InterruptedException {
          if (publisher != null) {
            publisher.shutdown();
            publisher.awaitTermination(1, TimeUnit.MINUTES);
          }
        }

        @Setup public void setup() throws IOException {
          dummy = ByteString.copyFromUtf8(ProtoUtils.encodeMessageBase64(
            SalesEvent.newBuilder().setId(DeviceId.newBuilder().setOs(OsType.OS_ANDROID).setWebid("dummy").build())
              .setBundle("dummy").setAmount(1).build()));

          topicName = TopicName.of(GCP_PROJECT_ID, MAIN_TOPIC_ID);
          publisher = Publisher.newBuilder(topicName).build();


          Random rnd = new Random(686);
          List<DeviceId> id = new ArrayList<>();
          List<String> bundle = new ArrayList<>();
          for (int i = 0; i < 5_000; i++) {
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
          for (int j = 0; j < 5_000; j++) {
            bundle.add("A." + getRandomString(rnd, 3));
          }
          SalesEvent.Builder se = SalesEvent.newBuilder();
          for (int i = 0; i < 100_000; i++) {
            se.setId(id.get(rnd.nextInt(id.size())));
            se.setBundle(bundle.get(rnd.nextInt(bundle.size())));
            se.setAmount(1000 + rnd.nextInt(20));
            data.add(ProtoUtils.encodeMessageBase64(se.build()));
          }
        }

        void pub(ByteString data) {
          PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
          try {
            cntPub++;
            String msgId = publisher.publish(pubsubMessage).get();
            if (cntPub % 100 == 1) {
              LOG.info("[pub] message id: {}, {} ", msgId, cntPub);
            }
          } catch (InterruptedException e) {
          } catch (ExecutionException e) {
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

        @ProcessElement public void process(ProcessContext c) {
          final long durationSec = c.element().getKey() * 60L;
          final long qps = c.element().getValue();

          long total = 0;
          int cntWindows = 0;

          final long t1 = Instant.now().getMillis();
          LOG.info("[process] starting up.. at t1 = {} ({})", t1, Instant.ofEpochMilli(t1));

          while ((Instant.now().getMillis() - t1) / 1000L / 60L <= 25) { // runs for up to 25 minutes.
            LOG.info("[waiting period] waiting... {}", Instant.now());
            while (true) {
              final long t = Instant.now().getMillis();
              long sec = (t / 1000L) % durationSec;
              if (sec == 0)
                break;
              else {
                fireAndWait(200L);
              }
            }
            cntWindows++;

            final long t2 = Instant.now().getMillis();
            LOG.info("[process] (windows {}) t2 = {} ({})", cntWindows, t2, Instant.ofEpochMilli(t2));

            int cnt = 0;
            Random rnd = new Random(68686);
            while (durationSec - (Instant.now().getMillis() / 1000L) % durationSec > 3) {
              final long begin = Instant.now().getMillis();
              for (long i = 0; i < qps; i++) {
                StringBuffer sb = new StringBuffer();
                for (int j = 0; j < 50; j++) {
                  if (j == 0)
                    sb.append(data.get(rnd.nextInt(data.size())));
                  else
                    sb.append(" " + data.get(rnd.nextInt(data.size())));
                }
                pub(ByteString.copyFromUtf8(sb.toString()));
                cnt++;
              }
              fireAndWait(begin + 800 - Instant.now().getMillis());
              LOG.info("[process] moving on... cnt = {}", cnt);
            }
            total += cnt;
            LOG.info("[process] published {} messages (grand total {})", cnt, total);
            c.output(String.format("%d,%d,%d", t1, t2, cnt));



          }
          LOG.info("[process] published {} messages (grand total)", total);
        }

      })).apply(TextIO.write().to(GCS_BUCKET + "/proj6/dummy-output"));

    p.run();
  }

  static MyOptions setDefaultValues(MyOptions options) {
    System.out.format("user.dir: %s", System.getProperty("user.dir"));

    options.setJobName("proj6-producer");

    options.setTempLocation(GCS_BUCKET + "/staging");
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
