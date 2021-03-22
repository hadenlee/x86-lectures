package edu.usfca.dataflow.L21;

import com.google.protobuf.Message;
import edu.usfca.protobuf.lectures.L21.L21.Numbers;
import edu.usfca.protobuf.lectures.L21.L21.Simple;
import edu.usfca.protobuf.lectures.L21.L21.Simple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoBasics {
  private static final Logger LOG = LoggerFactory.getLogger(ProtoBasics.class);

  static void printMessage(Message msg) {
    byte[] arr = msg.toByteArray();
    System.out.format("array length = %3d\n", arr.length);
    for (byte b : arr) {
      System.out.format("%02X ", b);
    }
    System.out.format("\n\n");
  }


  public static void run() {
    //    example2();
    //    example3();
    //    example4();
    example5();
  }

  private static void example5() {
    Numbers.Builder num = Numbers.newBuilder();

    int v;

    v = 10;
    num.setV1(v).setV2(v).setV3(v).setV4(v).setV5(v);
    printMessage(num.build());

    v = -10;
    num.setV1(v).setV2(v).setV3(v).setV4(v).setV5(v);
    printMessage(num.build());

    v = (1 << 29) + (1 << 30);
    num.setV1(v).setV2(v).setV3(v).setV4(v).setV5(v);
    printMessage(num.build());

    v = -v;
    num.setV1(v).setV2(v).setV3(v).setV4(v).setV5(v);
    printMessage(num.build());
  }

  private static void example4() {
    Simple.Builder msg1 = Simple.newBuilder();
    Simple2.Builder msg2 = Simple2.newBuilder();

    msg1.setId("CS").setVal(200);
    printMessage(msg1.build());

    msg2.setId("CS").setVal(200);
    printMessage(msg2.build());
  }

  private static void example3() {
    Simple.Builder msg = Simple.newBuilder();

    msg.setId("C"); // 67
    printMessage(msg.build());

    msg.setId("S"); // 83
    printMessage(msg.build());

    msg.setId("CS");
    printMessage(msg.build());

    msg.setId("USF"); // 85 83 70
    printMessage(msg.build());

    msg.setId("DATAFLOW"); // 85 83 70
    printMessage(msg.build());
  }

  private static void example2() {
    Simple.Builder msg = Simple.newBuilder();

    msg.setVal(100);
    printMessage(msg.build());

    msg.setVal(127);
    printMessage(msg.build());

    msg.setVal(128);
    printMessage(msg.build());

    msg.setVal(256 + 128 + 64 + 32);
    printMessage(msg.build());

    msg.setVal(3 * 128 * 128 * 128 + 5 * 128 * 128 + 7 * 128);
    printMessage(msg.build());
  }
}
