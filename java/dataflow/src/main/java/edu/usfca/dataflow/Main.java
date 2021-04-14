package edu.usfca.dataflow;


import edu.usfca.dataflow.proj6.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Code used in lectures.
 */
public class Main {
  private static final Logger LOG = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {
    // Change the following line to execute a specific main method.
    Producer.run(args);
  }
}
