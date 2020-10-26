package SecondarySorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortCompKeySortComparator extends WritableComparator {
	
	protected SecondarySortCompKeySortComparator() {
		
		super(CompositeKeyWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		if(key2.getTopic() == null || key1.getTopic() == null) {
			return -2;
		}
		int cmpResult = key2.getTopic().compareTo(key1.getTopic());
		
		if (cmpResult == 0)// same deptNo
		{
			return -key1.getTopic().compareTo(key2.getTopic());
			//If the minus is taken out, the values will be in ascending order
		}
		return cmpResult;
	}
}