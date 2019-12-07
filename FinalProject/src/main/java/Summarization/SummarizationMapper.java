package Summarization;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SummarizationMapper extends Mapper<LongWritable, Text, Text, DataWritable> {

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    DataWritable dataWritable = new DataWritable();

    if (!line.startsWith("Y")) {

      dataWritable.setMin_age(Integer.parseInt(tokens[11]));
      dataWritable.setMax_age(Integer.parseInt(tokens[11]));
      dataWritable.setMin_chol(Integer.parseInt(tokens[14]));
      dataWritable.setMax_chol(Integer.parseInt(tokens[14]));
      dataWritable.setMax_data_value(Float.parseFloat(tokens[7]));
      dataWritable.setMin_data_value(Float.parseFloat(tokens[7]));
      dataWritable.setMax_thalach(Integer.parseInt(tokens[17]));
      dataWritable.setMin_thalach(Integer.parseInt(tokens[17]));
      context.write(new Text(tokens[6]), dataWritable);
    }
  }
}
