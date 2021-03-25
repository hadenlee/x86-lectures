package edu.usfca.dataflow.L23;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import edu.usfca.dataflow.L21.ProtoBasics;
import edu.usfca.protobuf.lectures.L23.Msg1;
import edu.usfca.protobuf.lectures.L23.Msg2;
import edu.usfca.protobuf.lectures.L23.Msg3;
import edu.usfca.protobuf.lectures.L23.Msg4;
import edu.usfca.protobuf.lectures.L23.Phone1;

import java.util.Arrays;
import java.util.stream.Collectors;

public class ProtoVersioning {
  static void print(Message m) {
    if (m instanceof Msg1) {
      Msg1 decodedM1 = (Msg1) m;
      System.out.format("[msg1] name: %s val : %d arr : %s\n", decodedM1.getName(), decodedM1.getVal(),
        Arrays.toString(decodedM1.getArrList().toArray()));
    } else if (m instanceof Msg2) {
      Msg2 decodedM2 = (Msg2) m;
      System.out.format("[msg2] name: %s val : %d\n", decodedM2.getName(), decodedM2.getVal());
    } else if (m instanceof Msg3) {
      Msg3 decodedM3 = (Msg3) m;
      System.out.format("[msg3] enum vals: %s\n",
        decodedM3.getVList().stream().map(v -> v.toString()).collect(Collectors.joining(",")));

    } else if (m instanceof Msg4) {
      Msg4 decodedM4 = (Msg4) m;
      System.out.format("[msg4] enum vals: %s\n",
        decodedM4.getVList().stream().map(v -> v.toString()).collect(Collectors.joining(",")));
    }
  }

  public static void run() {
    //example1();
    //example2();
    //    example3();
    Msg3.Builder m3 = Msg3.newBuilder();
    m3.addV(Phone1.PHONE1_UNSPECIFIED).addV(Phone1.PHONE1_2G).addV(Phone1.PHONE1_WINDOWS);

    final byte[] data = m3.build().toByteArray();
    ProtoBasics.printMessage(m3.build());

    try {
      Msg4 m4 = Msg4.parseFrom(data);
      ProtoBasics.printMessage(
        m4); // <- Notice that even though "150" is an unrecognized enum value, it's preserved through serialization.

      print(m3.build());
      print(m4);

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }


  }

  static void example3() {
    Msg2.Builder m2 = Msg2.newBuilder();

    m2.setVal(2_000_000_000_000L);
    ProtoBasics.printMessage(m2.build());

    final byte[] data = m2.build().toByteArray();

    try {
      Msg1 decodedM1 = Msg1.parseFrom(data);
      print(decodedM1);

      Msg2 decodedM2 = Msg2.parseFrom(data);
      print(decodedM2);

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }

  static void example2() {
    Msg2.Builder m2 = Msg2.newBuilder();

    m2.setName("ABC").setVal(200);
    ProtoBasics.printMessage(m2.build());

    final byte[] data = m2.build().toByteArray();

    try {
      Msg1 decodedM1 = Msg1.parseFrom(data);
      print(decodedM1);

      Msg2 decodedM2 = Msg2.parseFrom(data);
      print(decodedM2);

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }

  static void example1() {
    Msg1.Builder m1 = Msg1.newBuilder();

    m1.setName("ABC").setVal(200);
    m1.addAllArr(Arrays.asList("A", "B", "C"));

    ProtoBasics.printMessage(m1.build());

    final byte[] data = m1.build().toByteArray();

    try {
      Msg1 decodedM1 = Msg1.parseFrom(data);
      print(decodedM1);

      Msg2 decodedM2 = Msg2.parseFrom(data);
      print(decodedM2);

    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }
}
