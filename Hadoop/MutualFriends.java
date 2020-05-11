import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Displays a mutual friend list of two friends
 * @author Anshul Pardhi
 *
 */
public class MutualFriends {

	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{

		private Text friendKey = new Text();
		private Text friendValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\\t"); //Split friend id from list of friends
		
			if(line.length == 2) {
				String[] friendList = line[1].split(","); //Split friends of a particular id
				for(String frnd : friendList) {
					//Store the keys pairs in a sorted order
					String first = null;
					String second = null;
					if(Integer.parseInt(line[0]) < Integer.parseInt(frnd)) {
						first = line[0];
						second = frnd;
					} else {
						first = frnd;
						second = line[0];
					}
					friendKey.set(first + ", " + second);
					friendValue.set(line[1]);
					context.write(friendKey, friendValue); //Keys-Value pairs will be like "0,1"-"list of friends of 0", "0,2"-"list of friends of 0"...."0,1"-"list of friends of 1",.....
				}
			}
		}
	}

	public static class Reduce
	extends Reducer<Text, Text, Text, Text> {

		private Text resultValue = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
						
			String keyText = key.toString();
			//The particular pairs asked in the question. Change/insert new pair to look out for mutual friends of that new pair
			if(keyText.equals("0, 1") || keyText.equals("20, 28193") || keyText.equals("1, 29826") || keyText.equals("6222, 19272") || keyText.equals("28041, 28056")) {
				
				HashSet<String> hs = new HashSet<>();
				StringBuffer sb = new StringBuffer();
				String prefix = "";
				
				for(Text value : values) {
					String[] friendVals = value.toString().split(",");
					for(String friendValue : friendVals) {
						//Add to final answer if hashset already contains the friend id
						if(!hs.contains(friendValue)) {
							hs.add(friendValue);
						} else {
							sb.append(prefix);
							prefix = ", ";
							sb.append(friendValue);
						}
					}
				}
				
				resultValue.set(sb.toString());
				context.write(key, resultValue);
			}
		}
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}

		// create a job with name "mutualfriends"
		Job job = new Job(conf, "mutualfriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		
		// set output value type
		job.setOutputValueClass(Text.class);
		
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}