package edu.usfca.dataflow.L22;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.usfca.dataflow.L21.ProtoBasics;
import edu.usfca.protobuf.lectures.L22.Dummy;
import edu.usfca.protobuf.lectures.L22.Entry;
import edu.usfca.protobuf.lectures.L22.Map1;
import edu.usfca.protobuf.lectures.L22.Map2;

public class ProtoAdvanced {

  public static void run() {
    Map1.Builder m1 = Map1.newBuilder();

    m1.putM1("AB", 10);
    ProtoBasics.printMessage(m1.build());
    m1.putM1("AB", 100);
    ProtoBasics.printMessage(m1.build());


    Map1 m2 = null;
    try {
      byte[] data =
        new byte[] {0x0A, 0x06, 0x0A, 0x02, 0x41, 0x42, 0x10, 0x0A, 0x0A, 0x06, 0x0A, 0x02, 0x41, 0x42, 0x10, 0x64};
      m2 = Map1.parseFrom(data);
      ProtoBasics.printMessage(m2);

      byte[] data2 =
        new byte[] {0x0A, 0x06, 0x0A, 0x02, 0x41, 0x42, 0x10, 0x64, 0x0A, 0x06, 0x0A, 0x02, 0x41, 0x42, 0x10, 0x0A};
      m2 = Map1.parseFrom(data2);
      ProtoBasics.printMessage(m2);
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }

  }

  static void example5() {
    Map1.Builder m1 = Map1.newBuilder();
    Map2.Builder m2 = Map2.newBuilder();

    m1.putM1("AB", 100);
    m2.addId("AB").addVal(100);
    ProtoBasics.printMessage(m1.build());
    ProtoBasics.printMessage(m2.build());

    m1.putM1("XY", 1000);
    m2.addId("XY").addVal(1000);
    ProtoBasics.printMessage(m1.build());
    ProtoBasics.printMessage(m2.build());
  }

  static void example4() {
    Entry.Builder e = Entry.newBuilder();
    Map1.Builder m = Map1.newBuilder();

    e.setId("AB").setVal(100);
    m.putM1("AB", 100);
    m.addM2(e);
    ProtoBasics.printMessage(m.build());

    m.clear();
    e.setId("XY").setVal(1000);
    m.putM1("XY", 1000);
    m.addM2(e);
    ProtoBasics.printMessage(m.build());
  }

  static void example3() {
    Entry.Builder e = Entry.newBuilder();
    Dummy.Builder d = Dummy.newBuilder();

    e.setId("A").setVal(10);
    d.addE2(e);
    ProtoBasics.printMessage(d.build());

    e.setId("B").setVal(100);
    d.addE2(e);
    ProtoBasics.printMessage(d.build());

    e.setId("C").setVal(50);
    d.addE2(e);
    ProtoBasics.printMessage(d.build());

    d.clear();
    e.setId("ABC").setVal(100);
    d.setE1(e);
    d.addE2(e);
    ProtoBasics.printMessage(d.build());
  }

  static void example2() {
    Entry.Builder e = Entry.newBuilder();
    Dummy.Builder d = Dummy.newBuilder();

    d.addV(10);
    ProtoBasics.printMessage(d.build());

    d.addV(100);
    ProtoBasics.printMessage(d.build());

    d.addV(1000);
    ProtoBasics.printMessage(d.build());

    d.addV(100).addV(10);
    ProtoBasics.printMessage(d.build());
  }

  static void example1() {
    Entry.Builder e = Entry.newBuilder();
    Dummy.Builder d = Dummy.newBuilder();

    d.setE1(e.setId("ABC").setVal(100));
    ProtoBasics.printMessage(d.build());

    d.setE1(e.setId("XYZ").setVal(1000));
    ProtoBasics.printMessage(d.build());

    d.setE1(e.setId("ABCXYZ").setVal(1000));
    ProtoBasics.printMessage(d.build());

  }
}
