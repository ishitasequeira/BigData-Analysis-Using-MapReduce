package Binning;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
    private MultipleOutputs<Text, NullWritable> mos = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        if (!value.toString().startsWith("ID")) {
          String[] input = value.toString().split(",");
          Text Name = new Text(input[6]);
          String line = Name.toString();
          if (line.equalsIgnoreCase("Acute Myocardial Infarction (Heart Attack)")) {
            mos.write("bins", value, NullWritable.get(), "AcuteMyocardialInfarction(HeartAttack)");
          } else if (line.equalsIgnoreCase("Cholesterol Abnormalities")) {
            mos.write("bins", value, NullWritable.get(), "CholesterolAbnormalities");
          } else if (line.equalsIgnoreCase("Coronary Heart Disease")) {
            mos.write("bins", value, NullWritable.get(), "CoronaryHeartDisease");
          } else if (line.equalsIgnoreCase("Diabetes")) {
            mos.write("bins", value, NullWritable.get(), "Diabetes");
          } else if (line.equalsIgnoreCase("Hypertension")) {
            mos.write("bins", value, NullWritable.get(), "Hypertension");
          } else if (line.equalsIgnoreCase("Major Cardiovascular Disease")) {
            mos.write("bins", value, NullWritable.get(), "MajorCardiovascularDisease");
          } else if (line.equalsIgnoreCase("Nutrition")) {
            mos.write("bins", value, NullWritable.get(), "Nutrition");
          } else if (line.equalsIgnoreCase("Obesity")) {
            mos.write("bins", value, NullWritable.get(), "Obesity");
          } else if (line.equalsIgnoreCase("Physical Inactivity")) {
            mos.write("bins", value, NullWritable.get(), "PhysicalInactivity");
          } else if (line.equalsIgnoreCase("Smoking")) {
            mos.write("bins", value, NullWritable.get(), "Smoking");
          } else if (line.equalsIgnoreCase("Stroke")) {
            mos.write("bins", value, NullWritable.get(), "Stroke");
          }
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        mos.close();
    }
}