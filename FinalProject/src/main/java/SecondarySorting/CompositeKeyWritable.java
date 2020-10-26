package SecondarySorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.Locale;
import java.text.ParseException;
import java.text.SimpleDateFormat; 

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	private String location;
	private String topic;
	
	public CompositeKeyWritable(){
		
	}
	
	public CompositeKeyWritable(String location,String topic){
		
		this.location =location;
		this.topic = topic;
	}
	
	public int compareTo(CompositeKeyWritable o) {
		
		int result = location.compareTo(o.location);
		if (result==0){
			result= topic.compareTo(o.topic);
		}
		return result;
	}

	public void write(DataOutput d) throws IOException {
		
		WritableUtils.writeString(d, location);
		WritableUtils.writeString(d, topic.toString());

	}

	public void readFields(DataInput di) throws IOException {
		
		location = WritableUtils.readString(di);
		topic = WritableUtils.readString(di);

	}
	
	public String getLocation() {
		return location;
	}


	public void setLocation(String location) {
		this.location = location;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public String toString() {
		return "CompositeKeyWritable{" +
				"location='" + location + '\'' +
				", topic='" + topic + '\'' +
				'}';
	}
}