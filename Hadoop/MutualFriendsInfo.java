import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Displays mutual friends info for the given pair of friends
 * @author Anshul Pardhi
 *
 */
public class MutualFriendsInfo {

// count word frequency and find the word id	
	
	public static class Map
	extends Mapper<LongWritable, Text, Text, Text>{
		
		HashMap<String,String> map = new HashMap<String,String>();
		private Text friendKey = new Text();
		private Text friendValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			
			String[] line = value.toString().split("\\t"); //Split friend id from list of friends
			//Only read line when id equals the one in console input
			if(line[0].equals(conf.get("userA")) || line[0].equals(conf.get("userB"))) {
				if(line.length == 2) {
					String[] friendList = line[1].split(",");
					for(String frnd : friendList) {
						friendKey.set(map.get(frnd));
						friendValue.set(line[0]);
						context.write(friendKey, friendValue); // ("NameAddress", "FriendID")
					}
				}
			}
		}
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the mapper.
			Configuration conf = context.getConfiguration();
		
			Path part=new Path(conf.get("userData"));//Location of file in HDFS
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	//put (word, wordID) in the HashMap variable
		        	if(arr.length == 10) {
		        		map.put(arr[0], arr[1] + ": " + arr[9]); // ("ID", "NameAddress")
		        	}
		        	line=br.readLine();
		        }
		    }
		}
	}
	
	public static class Reduce
	extends Reducer<Text,Text,Text,Text> {

		private Text resultKey = new Text();
		private Text resultValue = new Text();
		private StringBuffer globalSB = new StringBuffer("[");

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			List<Text> list = new ArrayList<>();
			int count = 0;
			for(Text value: values) {
				
				count++;
				if(count > 1) {
					list.add(key); // Mutual friend found!
				}
			}
			
			//Add mutual friend details to output list
			if(list.size() > 0) {
				for(Text listElem: list) {
					globalSB.append(listElem.toString() + ", ");
				}
			}
		}
		
		
		 @Override 
		 public void cleanup(Context context) throws IOException, InterruptedException {
		 
			 Configuration conf = context.getConfiguration();
			 resultKey.set(conf.get("userA") + " " + conf.get("userB"));
		 
			 //Formatting manipulation
			 int len = globalSB.length();
			 if(len > 2) {
				 globalSB.delete(len-2, len);
			 }
			 globalSB.append("]");
			 
			 resultValue.set(globalSB.toString());
			 context.write(resultKey, resultValue); 
		}
		 
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: MutualFriendsInfo <in> <in> <out> <userA> <userB>");
			System.exit(2);
		}
		
		// the userdata.txt input path
		conf.set("userData",otherArgs[1]);
		
		//the input user id's
		conf.set("userA", otherArgs[3]);
		conf.set("userB", otherArgs[4]);
		
		// create a job with name "mutualfriendsinfo"
		Job job = new Job(conf, "mutualfriendsinfo");
		job.setJarByClass(MutualFriendsInfo.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data (word.txt)
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path of the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}