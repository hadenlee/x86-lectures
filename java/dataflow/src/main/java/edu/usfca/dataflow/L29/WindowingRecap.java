package edu.usfca.dataflow.L29;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class WindowingRecap {
  public static void run() {
    //    example1();
    //    example2();
    //    example3();
    //    example4Acc();
    //    example5Dis(); // <- change 'accumulating' to 'discarding'.
    // exercise 6: just copy and paste example4Acc code, and then change the trigger object.
  }

  public static void example5Dis() {
    //(“B”, 3)	10:05AM
    //(“B”, 4)	10:08AM
    //(“B”, 7)	10:14AM
    //(“B”, 8)	10:21AM
    //(“B”, 10)	10:21AM
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts + 5 * minute, KV.of("B", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 14 * minute, KV.of("B", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("B", 10))//
    ));

    PCollection<KV<String, Integer>> timedData =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          if (c.element().getValue().getKey().equals("B"))
            c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    // --------------------

    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1));

    // TODO: Change the code to use discardingFiredPanes, and see what the results look like.

    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .accumulatingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())//
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] key: %s  values: [%s]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());

          c.output(KV.of(c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).reduce(0, Integer::sum)));
        }
      })).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
      @ProcessElement public void process(ProcessContext c) {
        if (c.element().getKey().equals("A")) {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 10));
        } else {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 100));
        }
      }
    }))//
      .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[DoFn] key: %s  value: [%d]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            c.element().getValue(), c.timestamp(), c.pane());
          c.output(c.element());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example4Acc() {
    //(“B”, 3)	10:05AM
    //(“B”, 4)	10:08AM
    //(“B”, 7)	10:14AM
    //(“B”, 8)	10:21AM
    //(“B”, 10)	10:21AM
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts + 5 * minute, KV.of("B", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 14 * minute, KV.of("B", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("B", 10))//
    ));


    PCollection<KV<String, Integer>> timedData =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          if (c.element().getValue().getKey().equals("B"))
            c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    // --------------------

    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1));

    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .accumulatingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())//
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] key: %s  values: [%s]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());

          c.output(KV.of(c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).reduce(0, Integer::sum)));
        }
      })).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
      @ProcessElement public void process(ProcessContext c) {
        if (c.element().getKey().equals("A")) {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 10));
        } else {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 100));
        }
      }
    }))//
      .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[DoFn] key: %s  value: [%d]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            c.element().getValue(), c.timestamp(), c.pane());
          c.output(c.element());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example3() {
    //(“A”, 1)	10:00AM
    //(“A”, 2)	10:03AM
    //(“B”, 3)	10:05AM
    //(“B”, 4)	10:08AM
    //(“A”, 5)	10:11AM
    //(“A”, 6)	10:13AM
    //(“B”, 7)	10:14AM
    //(“B”, 8)	10:21AM
    //(“A”, 9)	10:21AM
    //(“B”, 10)	10:21AM
    //(“A”, 11)	10:21AM
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("A", 2)),//
      KV.of(ts + 5 * minute, KV.of("B", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("A", 6)),//
      KV.of(ts + 14 * minute, KV.of("B", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9)),//
      KV.of(ts + 21 * minute, KV.of("B", 10)),//
      KV.of(ts + 21 * minute, KV.of("A", 11))//
    ));

    PCollection<KV<String, Integer>> timedData =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    // --------------------


    timedData.apply(Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10)))
      .withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())//
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] key: %s  values: [%s]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());

          c.output(KV.of(c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).reduce(0, Integer::sum)));
        }
      })).apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
      @ProcessElement public void process(ProcessContext c) {
        if (c.element().getKey().equals("A")) {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 10));
        } else {
          c.output(KV.of(c.element().getKey(), c.element().getValue() + 100));
        }
      }
    }))//
      .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[DoFn] key: %s  value: [%d]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            c.element().getValue(), c.timestamp(), c.pane());
          c.output(c.element());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example2() {
    //(“A”, 1)	10:00AM
    //(“A”, 2)	10:03AM
    //(“B”, 3)	10:05AM
    //(“B”, 4)	10:08AM
    //(“A”, 5)	10:11AM
    //(“A”, 6)	10:13AM
    //(“B”, 7)	10:14AM
    //(“B”, 8)	10:21AM
    //(“A”, 9)	10:21AM
    //(“B”, 10)	10:21AM
    //(“A”, 11)	10:21AM
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("A", 2)),//
      KV.of(ts + 5 * minute, KV.of("B", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("A", 6)),//
      KV.of(ts + 14 * minute, KV.of("B", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9)),//
      KV.of(ts + 21 * minute, KV.of("B", 10)),//
      KV.of(ts + 21 * minute, KV.of("A", 11))//
    ));

    PCollection<KV<String, Integer>> timedData =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    // --------------------


    timedData.apply(Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10)))
      .withAllowedLateness(org.joda.time.Duration.ZERO))
      .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          if (c.element().getKey().equals("A")) {
            c.output(KV.of(c.element().getKey(), c.element().getValue() + 10));
          } else {
            c.output(KV.of(c.element().getKey(), c.element().getValue() + 100));
          }
        }
      }))//
      .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c, BoundedWindow w) {
          System.out.format("[DoFn] key: %s  value: [%d]\ntimestamp:%s  pane: %s  window: %s\n\n", c.element().getKey(),
            c.element().getValue(), c.timestamp(), c.pane(), w.maxTimestamp());
          c.output(c.element());
        }
      })).apply(GroupByKey.create())//
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] key: %s  values: [%s]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example1() {
    //(“A”, 1)	10:00AM
    //(“A”, 2)	10:03AM
    //(“B”, 3)	10:05AM
    //(“B”, 4)	10:08AM
    //(“A”, 5)	10:11AM
    //(“A”, 6)	10:13AM
    //(“B”, 7)	10:14AM
    //(“B”, 8)	10:21AM
    //(“A”, 9)	10:21AM
    //(“B”, 10)	10:21AM
    //(“A”, 11)	10:21AM
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("A", 2)),//
      KV.of(ts + 5 * minute, KV.of("B", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("A", 6)),//
      KV.of(ts + 14 * minute, KV.of("B", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9)),//
      KV.of(ts + 21 * minute, KV.of("B", 10)),//
      KV.of(ts + 21 * minute, KV.of("A", 11))//
    ));

    PCollection<KV<String, Integer>> timedData =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    timedData.apply(Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10)))
      .withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] key: %s  values: [%s]\ntimestamp:%s  pane: %s\n\n", c.element().getKey(),
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }
}
