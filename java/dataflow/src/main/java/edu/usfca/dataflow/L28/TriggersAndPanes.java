package edu.usfca.dataflow.L28;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class TriggersAndPanes {
  public static void run() {
    //    example1Accumulate();
    //    example1Discard();
    example2Accumulate();
    example2Discard();
  }

  public static void example2Discard() { // Discarding panes

    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();
    List<KV<Long, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 19; i++) {
      int val = (1 << (i % 10));
      input.add(KV.of(ts + minute * i, val));
      System.out.format("%d ", val);
    }
    System.out.format("\n");

    PCollection<KV<String, Integer>> timedData =
      p.apply(Create.of(input)).apply(ParDo.of(new DoFn<KV<Long, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(KV.of("A", c.element().getValue()),
            org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));
    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(3));

    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .discardingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(Sum.integersPerKey())
      .apply(ParDo.of(new DoFn<KV<String, Integer>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          int sum = c.element().getValue();
          StringBuffer sb = new StringBuffer();
          while (sum > 0) {
            int val = sum & (sum ^ (sum - 1));

            sb.append(" " + val);
            sum -= val;
          }
          System.out
            .format("[SUM] sum: [%d from%s]\ntimestamp:%s  pane: %s\n\n", c.element().getValue(), sb.toString(),//
              c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }


  public static void example2Accumulate() { // Accumulating panes

    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();
    List<KV<Long, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 19; i++) {
      int val = (1 << (i % 10));
      input.add(KV.of(ts + minute * i, val));
      System.out.format("%d ", val);
    }
    System.out.format("\n");

    PCollection<KV<String, Integer>> timedData =
      p.apply(Create.of(input)).apply(ParDo.of(new DoFn<KV<Long, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(KV.of("A", c.element().getValue()),
            org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(3));
    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .accumulatingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(Sum.integersPerKey())
      .apply(ParDo.of(new DoFn<KV<String, Integer>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          int sum = c.element().getValue();
          StringBuffer sb = new StringBuffer();
          while (sum > 0) {
            int val = sum & (sum ^ (sum - 1));

            sb.append(" " + val);
            sum -= val;
          }
          System.out
            .format("[SUM] sum: [%d from%s]\ntimestamp:%s  pane: %s\n\n", c.element().getValue(), sb.toString(),//
              c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example1Discard() { // GBK-DoFn with Discarding panes

    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();
    List<KV<Long, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 19; i++) {
      input.add(KV.of(ts + minute * i, i + 1));
    }

    PCollection<KV<String, Integer>> timedData =
      p.apply(Create.of(input)).apply(ParDo.of(new DoFn<KV<Long, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(KV.of("A", c.element().getValue()),
            org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));
    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(3));

    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .discardingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] values: [%s]\ntimestamp:%s  pane: %s\n\n",
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }

  public static void example1Accumulate() { // GBK-DoFn with Accumulating panes

    Pipeline p = Pipeline.create();
    final long ts = Instant.parse("2021-05-01T10:00:00.000Z").toEpochMilli();
    final long minute = Duration.ofMinutes(1).toMillis();
    List<KV<Long, Integer>> input = new ArrayList<>();
    for (int i = 0; i < 19; i++) {
      input.add(KV.of(ts + minute * i, i + 1));
    }

    PCollection<KV<String, Integer>> timedData =
      p.apply(Create.of(input)).apply(ParDo.of(new DoFn<KV<Long, Integer>, KV<String, Integer>>() {
        @ProcessElement public void process(ProcessContext c) {
          c.outputWithTimestamp(KV.of("A", c.element().getValue()),
            org.joda.time.Instant.ofEpochMilli(c.element().getKey()));
        }
      }));

    Trigger trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(3));

    timedData.apply(
      Window.<KV<String, Integer>>into(FixedWindows.of(org.joda.time.Duration.standardMinutes(10))).triggering(trigger)
        .accumulatingFiredPanes().withAllowedLateness(org.joda.time.Duration.ZERO)).apply(GroupByKey.create())
      .apply(ParDo.of(new DoFn<KV<String, Iterable<Integer>>, Void>() {
        @ProcessElement public void process(ProcessContext c) {
          System.out.format("[GBK] values: [%s]\ntimestamp:%s  pane: %s\n\n",
            StreamSupport.stream(c.element().getValue().spliterator(), false).map(val -> val.toString())
              .collect(Collectors.joining(",")),//
            c.timestamp(), c.pane());
        }
      }));

    p.run().waitUntilFinish();
  }

}
