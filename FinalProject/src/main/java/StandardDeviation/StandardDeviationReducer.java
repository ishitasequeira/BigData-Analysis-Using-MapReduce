package StandardDeviation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StandardDeviationReducer extends Reducer<Text, DoubleWritable, Text, StandardDeviationWritable> {

	public List<Double> result = new ArrayList();
	public StandardDeviationWritable standardDeviationWritable = new StandardDeviationWritable();

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	double sum = 0;
	double count = 0;
	result.clear();

	standardDeviationWritable.setMedian(0);
	standardDeviationWritable.setSd(0);

	for (DoubleWritable doubleWritable : values) {
		sum = sum + doubleWritable.get();
		count = count + 1;
		result.add(doubleWritable.get());
	}

	Collections.sort(result);
	int length = result.size();
	double median = 0;
	if (length == 2) {
		double medianSum = result.get(0) + result.get(1);
		median = medianSum / 2;
	} else if (length % 2 == 0) {
		double medianSum = result.get((length / 2) -1) + result.get(length / 2);
		median = medianSum / 2;
	} else {
		median = result.get(length / 2);
	}
	standardDeviationWritable.setMedian(median);
		double mean = sum / count;
		double sumOfSquares = 0;
		for (double doubleWritable : result) {
		sumOfSquares += (doubleWritable - mean) * (doubleWritable - mean);
	}
	standardDeviationWritable.setSd((double) Math.sqrt(sumOfSquares / (count - 1)));
	context.write(key, standardDeviationWritable);
	}
}
