package edu.usfca.dataflow.L27;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.time.Duration;
import java.time.Instant;

public class WindowingExamples {

  public static void run() {
    //    example01();
    //    example02();
    //    example03();
    //    example04();
    example05();
  }

  static void example05() {
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("B", 2)),//
      KV.of(ts + 5 * minute, KV.of("A", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4))));

    PCollection<KV<String, Integer>> timedInput =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    PCollection<KV<String, Long>> transformedData =
      timedInput.apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, Long>>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out
            .format("[Original] key: %s  value: %d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
              c.timestamp().toString());

          c.output(KV.of("x" + c.element().getKey(), c.element().getValue().longValue()));
        }
      }));

    transformedData.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        System.out.format("[Updated] key: %s  value: %d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
          c.timestamp().toString());
      }
    }));

    p.run().waitUntilFinish();
  }

  static void example04() {
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("B", 2)),//
      KV.of(ts + 5 * minute, KV.of("A", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("B", 6)),//
      KV.of(ts + 14 * minute, KV.of("A", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9))//
    ));

    PCollection<KV<String, Integer>> timedInput =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    PCollection<KV<String, Integer>> windowedSum =
      timedInput.apply(Window.into(Sessions.withGapDuration(org.joda.time.Duration.standardMinutes(6L))))
        .apply(Sum.integersPerKey());

    windowedSum.apply("Windowed", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        System.out.format("[Windowed] key: %s  total: %2d  timestamp: %s  pane: %s\n", c.element().getKey(),
          c.element().getValue(), c.timestamp().toString(), c.pane().toString());
      }
    }));

    PCollection<KV<String, Integer>> justSum = timedInput.apply(Sum.integersPerKey());

    justSum.apply("JustSum", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        // By default, Beam SDK uses "global window" (whose 'closing time' is Long.MAX_VALUE).
        // As a result you will see: timestamp: 294247-01-09T04:00:54.775Z
        System.out.format("[Just Sum] key: %s  total: %2d  timestamp: %s  pane: %s\n", c.element().getKey(),
          c.element().getValue(), c.timestamp().toString(), c.pane().toString());
      }
    }));

    p.run().waitUntilFinish();
  }

  static void example03() {
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("B", 2)),//
      KV.of(ts + 5 * minute, KV.of("A", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("B", 6)),//
      KV.of(ts + 14 * minute, KV.of("A", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9))//
    ));

    PCollection<KV<String, Integer>> timedInput =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    PCollection<KV<String, Integer>> windowedSum =
      timedInput.apply(Window.into(Sessions.withGapDuration(org.joda.time.Duration.standardMinutes(6L))))
        .apply(Sum.integersPerKey());

    windowedSum.apply("Windowed", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        System.out
          .format("[Windowed] key: %s  total: %2d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
            c.timestamp().toString());
      }
    }));

    p.run().waitUntilFinish();
  }

  static void example02() {
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("B", 2)),//
      KV.of(ts + 5 * minute, KV.of("A", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("B", 6)),//
      KV.of(ts + 14 * minute, KV.of("A", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9))//
    ));

    PCollection<KV<String, Integer>> timedInput =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    PCollection<KV<String, Integer>> windowedSum = timedInput.apply(Window.into(
      SlidingWindows.of(org.joda.time.Duration.standardMinutes(5L)).every(org.joda.time.Duration.standardMinutes(3L))))
      .apply(Sum.integersPerKey());

    windowedSum.apply("Windowed", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        System.out
          .format("[Windowed] key: %s  total: %2d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
            c.timestamp().toString());
      }
    }));

    p.run().waitUntilFinish();
  }

  static void example01() {
    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();

    PCollection<KV<Long, KV<String, Integer>>> input = p.apply(Create.of(//
      KV.of(ts, KV.of("A", 1)),//
      KV.of(ts + 3 * minute, KV.of("B", 2)),//
      KV.of(ts + 5 * minute, KV.of("A", 3)),//
      KV.of(ts + 8 * minute, KV.of("B", 4)),//
      KV.of(ts + 11 * minute, KV.of("A", 5)),//
      KV.of(ts + 13 * minute, KV.of("B", 6)),//
      KV.of(ts + 14 * minute, KV.of("A", 7)),//
      KV.of(ts + 21 * minute, KV.of("B", 8)),//
      KV.of(ts + 21 * minute, KV.of("A", 9))//
    ));

    PCollection<KV<String, Integer>> timedInput =
      input.apply(ParDo.of(new DoFn<KV<Long, KV<String, Integer>>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          // By default, Beam SDK uses "Long.MIN_VALUE" for timestamp (millis).
          // As a result, you will see: timestamp: -290308-12-21T19:59:05.225Z
          System.out.format("[Before] key: %s  value: %2d  timestamp: %s\n", c.element().getValue().getKey(),
            c.element().getValue().getValue(), c.timestamp().toString());

          c.outputWithTimestamp(c.element().getValue(), org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    PCollection<KV<String, Integer>> windowedSum =
      timedInput.apply(Window.into(FixedWindows.of(org.joda.time.Duration.standardMinutes(5L))))
        .apply(Sum.integersPerKey());

    windowedSum.apply("Windowed", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        System.out
          .format("[Windowed] key: %s  total: %2d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
            c.timestamp().toString());
      }
    }));

    PCollection<KV<String, Integer>> justSum = timedInput.apply(Sum.integersPerKey());

    justSum.apply("JustSum", ParDo.of(new DoFn<KV<String, Integer>, Void>() {
      @ProcessElement public void process(ProcessContext c) {
        // By default, Beam SDK uses "global window" (whose 'closing time' is Long.MAX_VALUE).
        // As a result you will see: timestamp: 294247-01-09T04:00:54.775Z
        System.out
          .format("[Just Sum] key: %s  total: %2d  timestamp: %s\n", c.element().getKey(), c.element().getValue(),
            c.timestamp().toString());
      }
    }));

    p.run().waitUntilFinish();
  }
}
