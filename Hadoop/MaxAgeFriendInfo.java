import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
 * Displays friends of a user with the maximum age (top-10)
 * @author Anshul Pardhi
 *
 */
public class MaxAgeFriendInfo {
	
	//the first job returns the friend info with max age
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text friendKey = new Text();
		private Text friendValue = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] line = value.toString().split("\\t"); //Split friend id from list of friends
			if(line.length == 2) {
				
				friendKey.set(line[0]);
				String[] friendList = line[1].split(","); //Split friends of a particular id
				
				for(String frnd : friendList) {
					friendValue.set(frnd);
					context.write(friendKey, friendValue); //Simply pass the key as user id and value as friend id
				}
			}
		}
	} 

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

		private Text reduce1Key = new Text();
		private Text reduce1Value = new Text();
		private HashMap<String, Integer> hm = new HashMap<>();
		
		//Standard boilerplate code to calculate age from date of birth string
		private int calculateAge(String dob) throws ParseException {
			
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			Date birthDate = sdf.parse(dob);
			Date todayDate = new Date();
			
			DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			int bd = Integer.parseInt(formatter.format(birthDate));
			int td = Integer.parseInt(formatter.format(todayDate));
			
			return (td - bd)/10000;
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
		        	if(arr.length == 10) {
		        		try {
							hm.put(arr[0], calculateAge(arr[9])); // ("ID", Age)
						} catch (ParseException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
		        	}
		        	line=br.readLine();
		        }
		    }
		}
	
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int maxAge = 0;
		    for(Text value : values) {
		    	
		    	int age = hm.get(value.toString()); //Retrieve age from hashmap
		    	
		    	//Get the friend with max age
		    	maxAge = Math.max(maxAge, age);
		    }
		    
		    reduce1Key.set(String.valueOf(maxAge));
		    reduce1Value.set(key);
		    context.write(reduce1Key, reduce1Value); //("Max Age", "ID")
		}
	}
	
	//job2's mapper passes input straight as it is, the internal sort function then sorts the keys in descending order
	public static class Map2 extends Mapper<Text, Text, LongWritable, Text> {

		  private LongWritable map2Key = new LongWritable();
		  public void map(Text key, Text value, Context context)
		    throws IOException, InterruptedException {

			map2Key.set(Long.parseLong(key.toString()));
		    context.write(map2Key, value);
		  }
	}
	
	//Retrieve top 10 friends with max age, along with their addresses
	public static class Reduce2 extends Reducer<LongWritable, Text, Text, Text> {
		
		private Text reduce2Key = new Text();
		private Text reduce2Value = new Text();
		private int idx = 0;
		private HashMap<String, String> hm = new HashMap<>();
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
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
		        	if(arr.length == 10) {
		        		hm.put(arr[0], arr[3] + ", " + arr[4] + ", " + arr[5]); // ("ID", "Address")
		        	}
		        	line=br.readLine();
		        }
		    }
		}
 
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
			
			for (Text value : values) {
		    	if (idx < 10) {
		    		idx++;
		    		reduce2Key.set(value);
		    		String resValue = hm.get(value.toString()) + ", " + key.toString();
		    		reduce2Value.set(resValue);
		    		context.write(reduce2Key, reduce2Value); //("ID", "Address, Age")
		    	}
		    }
		}
	}
	
	
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 4) {
			System.err.println("Usage: MaxAgeFriendInfo <in> <in> <out2> <out1>");
			System.exit(2);
		}
        
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[2];
        String tempPath = otherArgs[3];
                
        //First Job
        {	//create first job
            conf = new Configuration();
            
            // the userdata.txt input path
         	conf.set("userData",otherArgs[1]);
            
            Job job = new Job(conf, "MaxAgeFriend");

            job.setJarByClass(MaxAgeFriendInfo.class);
            job.setMapperClass(MaxAgeFriendInfo.Map1.class);
            job.setReducerClass(MaxAgeFriendInfo.Reduce1.class);
            
            //set job1's mapper output key type
            job.setMapOutputKeyClass(Text.class);
            //set job1's mapper output value type
            job.setMapOutputValueClass(Text.class);
            
            // set job1;s output key type
            job.setOutputKeyClass(Text.class);
            // set job1's output value type
            job.setOutputValueClass(Text.class);
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
            
            // the userdata.txt input path
         	conf.set("userData",otherArgs[1]);
            
            Job job2 = new Job(conf, "MaxAgeFriendInfo");

            job2.setJarByClass(MaxAgeFriendInfo.class);
            job2.setMapperClass(MaxAgeFriendInfo.Map2.class);
            job2.setReducerClass(MaxAgeFriendInfo.Reduce2.class);
            
            //set job2's mapper output key type
            job2.setMapOutputKeyClass(LongWritable.class);
            //set job2's mapper output value type
            job2.setMapOutputValueClass(Text.class);
            
            //set job2's output key type
            job2.setOutputKeyClass(Text.class);
            //set job2's output value type
            job2.setOutputValueClass(Text.class);

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
