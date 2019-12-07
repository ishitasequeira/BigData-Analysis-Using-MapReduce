package StandardDeviation;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StandardDeviationMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private Text abbr = new Text(); // location abbr
  private DoubleWritable dataValue = new DoubleWritable(0);

  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    String data = value.toString();
    String[] field = data.split(",");
    if (!value.toString().startsWith("Y")) {
      double data_value = 0;
      if(!field[8].isEmpty())
	data_value = Double.parseDouble(field[8]);
      dataValue.set(data_value);
      abbr.set(field[1]);
      context.write(abbr, dataValue);
    }
  }
}
