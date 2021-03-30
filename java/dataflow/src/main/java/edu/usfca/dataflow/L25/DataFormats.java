package edu.usfca.dataflow.L25;

import com.google.openrtb.OpenRtb.BidRequest;
import com.google.openrtb.OpenRtb.BidRequest.Builder;
import com.google.openrtb.OpenRtb.BidRequest.Device;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataFormats {

  static List<String> allBadv = Arrays
    .asList("Lorem", "ipsum", "dolor", "sit", "amet,", "consectetur", "adipiscing", "elit,", "sed", "do", "eiusmod",
      "tempor", "incididunt", "ut", "labore", "et", "dolore", "magna", "aliqua.", "Ut", "enim", "ad", "minim",
      "veniam,", "quis", "nostrud", "exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea",
      "commodo", "consequat.", "Duis", "aute", "irure", "dolor", "in", "reprehenderit", "in", "voluptate", "velit",
      "esse", "cillum", "dolore", "eu", "fugiat", "nulla", "pariatur.", "Excepteur", "sint", "occaecat", "cupidatat",
      "non", "proident,", "sunt", "in", "culpa", "qui", "officia", "deserunt", "mollit", "anim", "id", "est",
      "laborum.");

  static List<String> allBapp = Arrays
    .asList("Velit", "sed", "ullamcorper", "morbi", "tincidunt", "ornare.", "At", "auctor", "urna", "nunc", "id",
      "cursus", "metus", "aliquam", "eleifend", "mi.", "Sit", "amet", "facilisis", "magna", "etiam", "tempor", "orci",
      "eu.", "Praesent", "elementum", "facilisis", "leo", "vel", "fringilla", "est", "ullamcorper", "eget", "nulla.",
      "Ac", "turpis", "egestas", "integer", "eget", "aliquet", "nibh", "praesent.", "A", "diam", "sollicitudin",
      "tempor", "id", "eu", "nisl", "nunc", "mi", "ipsum.", "Sed", "viverra", "ipsum", "nunc", "aliquet", "bibendum",
      "enim", "facilisis.", "Nulla", "aliquet", "porttitor", "lacus", "luctus.", "Sed", "faucibus", "turpis", "in",
      "eu", "mi.", "Turpis", "egestas", "sed", "tempus", "urna", "et", "pharetra", "pharetra.", "In", "nisl", "nisi",
      "scelerisque", "eu", "ultrices", "vitae", "auctor.");

  static List<String> allBcat = Arrays
    .asList("Morbi", "tincidunt", "augue", "interdum", "velit", "euismod", "in", "pellentesque.", "Ut", "etiam", "sit",
      "amet", "nisl", "purus", "in", "mollis", "nunc", "sed.", "Laoreet", "sit", "amet", "cursus", "sit", "amet",
      "dictum.", "Sapien", "et", "ligula", "ullamcorper", "malesuada", "proin", "libero", "nunc", "consequat",
      "interdum.", "Orci", "porta", "non", "pulvinar", "neque.", "Egestas", "dui", "id", "ornare", "arcu", "odio", "ut",
      "sem", "nulla.", "In", "fermentum", "posuere", "urna", "nec", "tincidunt.", "Risus", "sed", "vulputate", "odio",
      "ut", "enim", "blandit.", "Felis", "eget", "nunc", "lobortis", "mattis", "aliquam", "faucibus", "purus", "in.",
      "Amet", "est", "placerat", "in", "egestas.", "Nec", "sagittis", "aliquam", "malesuada", "bibendum", "arcu",
      "vitae.", "Cursus", "eget", "nunc", "scelerisque", "viverra", "mauris", "in", "aliquam", "sem", "fringilla.",
      "Morbi", "tristique", "senectus", "et", "netus", "et", "malesuada", "fames", "ac.", "Risus", "pretium", "quam",
      "vulputate", "dignissim", "suspendisse", "in", "est", "ante.", "Massa", "placerat", "duis", "ultricies", "lacus",
      "sed", "turpis.", "Facilisi", "morbi", "tempus", "iaculis", "urna", "id", "volutpat", "lacus.", "Massa", "tempor",
      "nec", "feugiat", "nisl.");

  static List<String> allCur = Arrays
    .asList("6bc2af68-ee7d-4afc-ac5c-d8727fed2309", "e657bc60-9575-499a-bd46-e4b48364e45c",
      "31ccea30-4007-4215-a3dc-32b1076a9d87", "c85acfef-ecd1-4b3d-8847-f88f099ee3e2",
      "1bc08330-71a8-47fc-8f95-7b6388ac37c6", "32296c7f-c534-4b86-8d60-7dc1e654284d",
      "2f69b291-8bf3-4e6c-b96c-131e73b70575", "19a892c0-480a-4dbb-b075-3edda57272a4",
      "119ed081-3ecb-49e8-9ae0-165f4b10d35a", "da183230-d1e2-40ca-8a7e-4421ae55551b",
      "84f32def-41a0-4692-9307-9d31e01d7be6", "096d6eec-1ec6-4622-885e-2183cd6d0292",
      "774d0594-73ad-4dff-adac-271e75e2622d", "a153373d-0260-4882-95f5-9f1450c99703",
      "4aa35964-9ec0-43f6-8e98-d8079b56b4bd", "ae015790-e1dd-4197-96c4-f7487e4e02a8",
      "6ed988aa-cc7b-47a7-91c4-fc7f8c020292", "da2d6c52-d5a2-43a4-a2e0-366b675e942e",
      "90795fa7-75fc-48ea-b941-436cc6731fe1", "f5b839c4-506a-49b3-abbf-4fbbf4b23b68",
      "a10ee29b-1a6b-46a3-a668-6576ff6f4722", "60b3cbc8-6e13-4f10-a9af-c083fa8651ac",
      "e5b7bc99-5789-4513-b906-555c67767965", "659dc6bc-22c5-440e-bb02-2596fd28bf2d",
      "e1325784-871c-4343-9836-1f4fddb71060", "da03d8fc-2713-4435-b0f6-1c80f95e872b",
      "4a2abf63-3e20-4049-96b3-dd043ebb4da8", "ea02d440-0988-4c04-a4a4-83b97d817ceb",
      "1318b82c-2a75-4728-962f-a4f8fd698284", "bfe19704-f9dd-4d40-9c98-e5a81cf84d04",
      "44c0243e-519f-4f1f-9a95-c6aecad1c5a2", "4979ccd0-6c08-430f-b020-32d133d85dc9",
      "f29cc23c-4fb2-4efe-9f44-21c4c65efb5c", "2f3f14c9-db9c-4213-9d43-7dc63f4352f3",
      "b6e23753-ec83-43bc-b7e8-5c8be0f74936", "7575dfd2-0412-4838-b4df-cd61541ea788",
      "99004f21-1cf7-4c4c-b757-b496856a02a5", "c20ccfb0-e147-4ac4-beb7-9b8c07b94baf",
      "2c544ae4-d4b9-47a0-893d-33414eb64bf9", "79da0d94-c61d-480c-9dce-72df63f00a5f",
      "a1afaa6c-5446-4164-b57c-a464cbbf0d94", "27bf59fe-81c3-449f-8473-26c0a8931891",
      "6161e10d-8980-4d25-8cb6-a791fd5b464c", "203c44d8-df4a-460d-b3f4-4d79eada5d08",
      "01878edd-5121-4a6f-8401-767eacd156f3", "39d98492-e421-4867-8669-c76344fe9db9",
      "0ea98274-f4a8-4579-bd8a-829ebe27db9a", "e65bfcc5-980d-47db-97ef-cbe4ab7cb952",
      "25de9c04-689a-4559-a6d0-b0905d40a358", "ccd280fb-8dde-49dd-9c68-969bf131e08b",
      "946b3cc3-2fc7-4196-b362-550c958306d4", "60d71b59-1c7a-44bb-b1e6-51674db8b6f1",
      "c50732f9-5d0b-4c7c-b2e4-aa9a482cc9d3", "f58cc058-f13d-4929-a7e4-1e0e07f082dc",
      "84fd6388-8936-4127-8e7c-6b388148b5b1", "d939cc4f-7f04-4423-a170-051a7990e669",
      "d11e20a3-5ab8-4c05-b1e9-c84f3dc0375e", "d8c4caff-eb36-46eb-8627-eeb11bd165c2",
      "63be09dd-479a-4bea-aa25-ef6388bca6fe", "8720c022-0500-455b-9814-4203b35e261a",
      "a713facd-e728-44ad-899b-eafdefb52841", "8bb6643f-b8f7-4ce0-9029-b5cc89c5b263",
      "76e04f6a-fcbc-43d3-a023-459cffa0a6fe", "68e84c50-b043-41dc-bfc3-e7dbb24be9fd",
      "a8d62b4e-3d15-47a1-8b38-1bc87046015e", "9ae9b7fa-dd5e-4609-9f7b-cfbe2ab46889",
      "c189bed1-a4da-451f-a0ad-0557a7b63413", "ceddcfa0-8f97-4987-aeac-18436e6d9adf",
      "24d21627-b151-4e69-956e-31bc9cf1ff1a", "3d25bc0c-087f-444c-8097-a37d4798cbf5",
      "c59dcf13-2456-4f5c-ae28-bc603898d659", "1ff19179-dedb-41b2-b9a0-995a2ce436c5",
      "220e3f69-6612-4374-b72e-49077c7a7487", "b288adbf-2f75-4bdc-a5b7-a4f5e892a28d",
      "938d32e8-8db3-4231-8a27-351337bbeff2", "d12fef5a-8afa-4b51-8dcf-7b4ac75ac207",
      "5217c70e-ae87-4176-baa7-15f16e9d40d2", "85f12c95-9702-4c46-aefc-89b80e3977ba",
      "8e9d463b-eb45-456d-97a0-abe242fb0826", "c5fc2359-6cb6-4513-9a10-a4c710a21491",
      "ad772909-61b9-4b1d-9e76-362dc3dfbad7", "fa4286c2-faaa-4f7a-9c65-4b0ba3fbbf55",
      "0a019944-1ab1-489c-9b2d-01b8e79f9e09", "54f3ad13-c3e0-47e7-a122-81910ad80ecd",
      "f0177cab-f50a-478e-b8dd-1554c8f9ebe8", "60210d96-2502-4a30-b5aa-055ace5b34a5",
      "94d72e8b-cf1d-4bd3-be6b-b12e0586e6b4", "a66f4210-84f0-44d6-be76-5cb0fa346f8d",
      "287c6571-b5cd-495f-99b2-1b41ea28216d", "f3f7bd7a-d7a6-4a9b-9f22-c4dcc861687b",
      "cf096aa8-4b2b-4b1c-b484-10a30df2c3ca", "0361d35d-7edc-4741-aaa8-b3e0cd22d9bc",
      "eb43e4d7-47e9-4788-a009-1b197f1f2b29", "02bb7cb6-2e2b-49e8-9847-d86c98b62055",
      "ef42d774-c932-44a0-af82-a8a920961521", "c97b1b05-ee36-48f8-a89a-6d73bf701956",
      "4df98e16-23e7-4a2e-9412-31673c3dbdb0", "40ede454-f2f0-4b1a-bd05-96b0bda8f456",
      "c91b5e12-4af8-40cb-b276-f9e9237d7a1e", "689195a0-fead-4a84-be7a-8beece261568");

  static byte[] getSampleBidRequest() {
    BidRequest.Builder br = BidRequest.newBuilder();
    br.setId("usf");
    br.setDevice(Device.newBuilder().setOs("ios").setIfa("895feb16-0082-4e39-9ee1-c8a29d0244af"));

    // Useless data that we do not really need:
    // (Note that the data is dummy and only for demo purposes.)
    br.addAllBadv(allBadv);
    br.addAllBapp(allBapp);
    br.addAllBcat(allBcat);
    br.addAllCur(allCur);

    return br.build().toByteArray();
  }

  static Random rnd = new Random();

  static byte[] getRandomBidRequest() throws InvalidProtocolBufferException {
    Builder br = BidRequest.newBuilder();

    br.setId(UUID.randomUUID().toString());
    br.setDevice(Device.newBuilder().setOs(rnd.nextBoolean() ? "iOS" : "Android").setIfa(UUID.randomUUID().toString()));

    // Useless data that we do not really need:
    // (Note that the data is dummy and only for demo purposes.)
    br.addAllBadv(allBadv.stream().filter(i -> rnd.nextBoolean()).collect(Collectors.toList()));
    br.addAllBapp(allBapp.stream().filter(i -> rnd.nextBoolean()).collect(Collectors.toList()));
    br.addAllBcat(allBcat.stream().filter(i -> rnd.nextBoolean()).collect(Collectors.toList()));
    br.addAllCur(allCur.stream().filter(i -> rnd.nextBoolean()).collect(Collectors.toList()));

    // System.out.format("%s\n", ProtoUtils.getJsonFromMessage(br.build(), true));
    return br.build().toByteArray();
  }

  /**
   * Compare the size of the four files produced.
   * <p>
   * The actual sizes may differ (as the data is randomly generated).
   */
  // Here is what my results look like, after running this code:
  //*[master][~/usf/cs686/x86-lectures/java]$ ls -lh dataflow/resources/L25
  //total 200
  //-rw-r--r--  1 haden  staff    40K Mar 29 19:44 base64-00000-of-00001.txt
  //-rw-r--r--  1 haden  staff    14K Mar 29 19:44 base64-00000-of-00001.txt.gz
  //-rw-r--r--  1 haden  staff    30K Mar 29 19:44 tfrecord-00000-of-00001.bin
  //-rw-r--r--  1 haden  staff   6.6K Mar 29 19:44 tfrecord-00000-of-00001.bin.gz
  public static void executeSampleCode() throws InvalidProtocolBufferException {
    List<byte[]> data = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      data.add(getRandomBidRequest());
    }

    Pipeline p = Pipeline.create();
    PCollection<byte[]> pc = p.apply(Create.of(data));

    // Write to plain-text files (line-delimited) using B64 encoding.
    pc.apply(MapElements.into(TypeDescriptors.strings()).via((byte[] x) -> Base64.getEncoder().encodeToString(x)))
      .apply(TextIO.write().to("dataflow/resources/L25/base64").withNumShards(1).withSuffix(".txt"));

    // Write to binary files using TFRecord.
    pc.apply(TFRecordIO.write().to("dataflow/resources/L25/tfrecord").withNumShards(1).withSuffix(".bin"));

    // repeat, with compression.

    // Write to plain-text files (line-delimited) using B64 encoding.
    pc.apply(MapElements.into(TypeDescriptors.strings()).via((byte[] x) -> Base64.getEncoder().encodeToString(x)))
      .apply(TextIO.write().to("dataflow/resources/L25/base64").withCompression(Compression.GZIP).withNumShards(1)
        .withSuffix(".txt"));

    // Write to binary files using TFRecord.
    pc.apply(TFRecordIO.write().to("dataflow/resources/L25/tfrecord").withCompression(Compression.GZIP).withNumShards(1)
      .withSuffix(".bin"));

    p.run().waitUntilFinish();
  }

  public static void run() {
    try {
      executeSampleCode();
    } catch (InvalidProtocolBufferException e) {
      e.printStackTrace();
    }
  }
}
