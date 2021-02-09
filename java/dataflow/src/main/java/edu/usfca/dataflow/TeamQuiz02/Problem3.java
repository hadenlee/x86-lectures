package edu.usfca.dataflow.TeamQuiz02;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class Problem3 {

  // This is a custom Java class to represent my message (data).
  // TODO - Problem 3:
  // If you try to run test3(), it will fail due to exception.
  // It says (among other things) -- "No Coder has been manually specified;  you may do so using .setCoder()."
  // This can be fixed by explicitly stating that "MyMessage implements" some interface.
  // (a) What is the interface that you need to 'implement'?
  // After you add that, test3() will fail again but for a different reason.
  // You will likely also observe a lot of warnings.
  // (b) You need to implement a method to resolve this issue. What is it?
  // (c) When you implement the method in (b), you must also implement another method
  // (although, even if you don't, the test will likely pass). What is it?
  static class MyMessage {
    final String key;
    final int value;

    public MyMessage() {
      this.key = "";
      this.value = 0;
    }

    public MyMessage(KV<String, Integer> kv) {
      this.key = kv.getKey();
      this.value = kv.getValue();
    }
  }


  public static class MyDoFn3 extends DoFn<KV<String, Integer>, MyMessage> {
    final int param;

    public MyDoFn3(int param) {
      this.param = param;
    }

    @ProcessElement public void process(ProcessContext c) {
      if (c.element().getValue() >= param) {
        c.output(new MyMessage(c.element()));
      }
    }
  }
}
