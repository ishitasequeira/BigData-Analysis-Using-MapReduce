package SecondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator() {
		super(CompositeKeyWritable.class,true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1,WritableComparable w2){
		
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		int cmpResult = key1.getLocation().compareTo(key2.getLocation());
		
		return cmpResult;
	}

}
