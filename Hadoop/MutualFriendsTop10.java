import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Top-10 mutual friend pairs
 * @author Anshul Pardhi
 *
 */
public class MutualFriendsTop10 {
// find top 10 mutual friends
	
	//the first job is a standard mutual friend count program
	public static class WordMapper extends Mapper<LongWritable, Text, Text, Text> {

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

	public static class SumReducer extends Reducer<Text, Text, Text, IntWritable> {

		private IntWritable count = new IntWritable();

	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		    int mutualFriendCount = 0;
		    HashSet<String> hs = new HashSet<>();
		    for(Text value : values) {
		    	String[] friendVals = value.toString().split(",");
		    	//Mutual friend exists if the friend id is already present in the hashset
		    	for(String friendValue : friendVals) {
		    		if(!hs.contains(friendValue)) {
		    			hs.add(friendValue);
		    		} else {
		    			mutualFriendCount++;
		    		}
		    	}
		    }
		    count.set(mutualFriendCount);
		    context.write(key, count);
		  }
	}
	
	//job2's mapper swap key and value, sort by key (the count of mutual friends).
	public static class WordMapper2 extends Mapper<Text, Text, LongWritable, Text> {

		  private LongWritable frequency = new LongWritable();

		  public void map(Text key, Text value, Context context)
		    throws IOException, InterruptedException {

		    int newVal = Integer.parseInt(value.toString());
		    frequency.set(newVal);
		    context.write(frequency, key);
		  }
	}
	
	//output the top 10 mutual friends 
	public static class SumReducer2 extends Reducer<LongWritable, Text, Text, LongWritable> {
		private int idx = 0;


		 
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
			
			for (Text value : values) {
		    	if (idx < 10) {
		    		idx++;
		    		context.write(value, key);
		    	}
		    }
		}
	}
	
	
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        String tempPath = otherArgs[2];
        //First Job
        {	//create first job
            conf = new Configuration();
            Job job = new Job(conf, "MutualFriendsTop10");

            job.setJarByClass(MutualFriendsTop10.class);
            job.setMapperClass(MutualFriendsTop10.WordMapper.class);
            job.setReducerClass(MutualFriendsTop10.SumReducer.class);
            
            //set job1's mapper output key type
            job.setMapOutputKeyClass(Text.class);
            //set job1's mapper output value type
            job.setMapOutputValueClass(Text.class);
            
            // set job1;s output key type
            job.setOutputKeyClass(Text.class);
            // set job1's output value type
            job.setOutputValueClass(IntWritable.class);
            //set job1's input HDFS path
            FileInputFormat.addInputPath(job, new Path(inputPath));
            //job1's output path
            FileOutputFormat.setOutputPath(job, new Path(tempPath));

            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        //Second Job
        {
            conf = new Configuration();
            Job job2 = new Job(conf, "TopCount");

            job2.setJarByClass(MutualFriendsTop10.class);
            job2.setMapperClass(MutualFriendsTop10.WordMapper2.class);
            job2.setReducerClass(MutualFriendsTop10.SumReducer2.class);
            
            //set job2's mapper output key type
            job2.setMapOutputKeyClass(LongWritable.class);
            //set job2's mapper output value type
            job2.setMapOutputValueClass(Text.class);
            
            //set job2's output key type
            job2.setOutputKeyClass(Text.class);
            //set job2's output value type
            job2.setOutputValueClass(LongWritable.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
            //hadoop by default sorts the output of map by key in ascending order, set it to decreasing order
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setNumReduceTasks(1);
            //job2's input is job1's output
            FileInputFormat.addInputPath(job2, new Path(tempPath));
            //set job2's output path
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
