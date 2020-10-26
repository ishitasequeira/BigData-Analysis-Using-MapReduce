package SecondarySorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, IntWritable> {
	
	@SuppressWarnings("unused")
	public void reduce(CompositeKeyWritable key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
		int count = 0;
		for(NullWritable value :values){
			count ++;		
		}
		try {
			context.write(key, new IntWritable(count));
		} catch (Exception e) {
			// TODO: handle exception
		}

	}
}