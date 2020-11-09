import java.io.*; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Reducer; 

public class least_5_reducer extends Reducer<LongWritable, Text, LongWritable, Text> { 

	static int count; 

	@Override
	public void setup(Context context) throws IOException, InterruptedException 
	{ 
		count = 0; 
	} 

	@Override
	public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{ 

		// key				 values 
		//-ve of no_of_views [ movie_name ..] 
		long no_of_views = key.get(); //remove -1, orders lowest keys

		String movie_name = null; 

		for (Text val : values) 
		{ 
			movie_name = val.toString(); 
		} 

		if (count < 5) //least five, write 10 records
		{ 
			context.write(new LongWritable(no_of_views), 
								new Text(movie_name)); 
			count++; 
		} 
	} 
} 