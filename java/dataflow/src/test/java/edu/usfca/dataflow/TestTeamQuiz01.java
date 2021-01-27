package edu.usfca.dataflow;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTeamQuiz01 {
  @Rule public final transient TestPipeline tp = TestPipeline.create();

  @Test public void teamQuiz_1() {
    //---------------------------------------------------------------------------------------------
    PCollection<String> A = tp.apply("A", Create.of("data", "processing"));
    PCollection<String[]> B = tp.apply("B", Create.of(new String[] {"data", "processing"}));
    //---------------------------------------------------------------------------------------------

    PAssert.that(A).containsInAnyOrder("processing", "data");
    // Problem 1: The following line as-is won't work. How to fix it?
    //PAssert.that(B).containsInAnyOrder("data", "processing");

    tp.run(); // <- This "triggers" JVM to execute PAssert() statements from earlier.
  }

  @Test public void teamQuiz_2() {
    // Problem 2: The following line fails, even though two arrays contain the same values (in the same order).
    // If we want to (logically) treat them equal, how can we fix this? (this may be useful in future assignments).
    // Note: There are many ways to achieve this, and any reasonably efficient solution will be accepted.
    // Converting an array into String (e.g., using Arrays.toString()) isn't considered efficient.
    assertEquals(new int[] {686, 1, 3, 5}, new int[] {686, 1, 3, 5});
  }

  @Test public void teamQuiz_3() {
    //---------------------------------------------------------------------------------------------
    // Problem 3: The following line as-is won't work. *WHY* is this causing a compile error?
    // PCollection<int> G = tp.apply("G", Create.of(999));
    PCollection<int[]> H = tp.apply("H", Create.of(new int[] {999}));
    //---------------------------------------------------------------------------------------------

    // Problem 4 (optional): From 'teamQuiz_2()' above, we know that comparing two int[]'s directly (using assertEquals())
    // does not work. However, the following line passes because "containsInAnyOrder()" does take into account
    // values stored in arrays.
    // How does it do it? (Specifically, which Java libraries does Beam SDK use to do this easily?)
    // You will need to read some code in Beam SDK by following its declaration (what fun!).
    PAssert.that(H).containsInAnyOrder(new int[] {999});

    tp.run(); // <- This "triggers" JVM to execute PAssert() statements from earlier.
  }
}
