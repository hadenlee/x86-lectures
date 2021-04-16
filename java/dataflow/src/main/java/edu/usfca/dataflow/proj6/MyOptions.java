package edu.usfca.dataflow.proj6;


import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

public interface MyOptions extends DataflowPipelineOptions {
  @Description("Job name when running on GCP") @Default.String("cs686-proj6") String getJob();

  void setJob(String job);

  @Description("DirectRunner will be used if true") @Default.Boolean(true) boolean getIsLocal();

  void setIsLocal(boolean value);

  @Description("window size in minutes.") @Default.Integer(0) int getDuration();

  void setDuration(int duration);

  @Description("max messages per second.") @Default.Integer(0) int getQps();

  void setQps(int qps);

  @Description("full scale if true") @Default.Boolean(false) boolean getFullScale();

  void setFullScale(boolean value);
}

