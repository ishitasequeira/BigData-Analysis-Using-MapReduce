package SecondarySorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKeyWritable, NullWritable > {

	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value, int numReduceTasks) {
		
		int hash = key.getLocation().hashCode();
		int partition = hash % numReduceTasks;
		
		return partition;
	}
}