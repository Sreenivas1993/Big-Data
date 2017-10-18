import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class UserReview extends Configured implements Tool{
	//Map class
	public static class Map extends Mapper<LongWritable,Text,Text,PairWritable>{
		private Text user=new Text();
		private PairWritable rating=new PairWritable();
		private Set<String> businessid=new HashSet<String>();
		private FileSystem fs=null;
		@Override
		protected void setup(Context context)throws IOException,InterruptedException{
			try{
				String line=null;
				String bid=null;
				String[] fields=null;
				FileSystem fs=FileSystem.getLocal(context.getConfiguration());
				Path[] businessfiles=DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(fs.open(businessfiles[0])));
				while((line=bufferedReader.readLine())!=null){
					fields=line.split("::");
					if(fields[1].contains("Palo Alto")){
						businessid.add(fields[0]);
					}
				}
			}
			catch(IOException ex) {
				System.err.println("Exception in mapper setup: "
						+ ex.getMessage());
			}
		}
		//Map file
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String[] reviewfields=value.toString().split("::");
			float sum=0;
			int count=0;
			if(businessid.contains(reviewfields[2])){
				sum=sum+Float.parseFloat(reviewfields[3]);
				count+=1;
				rating.set(sum, count);
				context.write(new Text(reviewfields[1]),rating);
			}
		}
	}
	//combiner class
	public static class CombineClass extends Reducer<Text,PairWritable,Text,PairWritable>{
		private PairWritable value=new PairWritable();
		public void combine(Text key,Iterable<PairWritable>values,Context context)throws IOException,InterruptedException{
			float sum=0;
			int count=0;
			for(PairWritable pair:values){
				sum+=pair.getStar();
				count+=pair.getCount();
			}
			value.set(sum, count);
			context.write(key,value);
		}
		
	}
	//Reduce class
	public static class Reduce extends Reducer<Text,PairWritable,Text,Text>{
		Text keyval=new Text();
		Text averageresult=new Text();
		public void reduce(Text key,Iterable<PairWritable> values,Context context)throws IOException,InterruptedException{
			float sum=0;
			int count=0;
			for(PairWritable value:values){
				sum=sum+value.getStar();
				count+=value.getCount();
			}
			float averesult=new Float(sum/count);
			keyval.set(key);
			averageresult.set(Float.toString(averesult));
			context.write(keyval, averageresult);	
		}
	}
	@Override
	public int run(String[]args)throws Exception{
		if (args.length != 3) {
			System.err.printf("Usage: %s needs two arguments <input> <output> <usagereview> files\n",
							getClass().getSimpleName());
			return -1;
		}
		Configuration conf=new Configuration();
		//Initializing hadoop job
		Job job=new Job();
		job.setJobName("User review");
		job.setJarByClass(UserReview.class);
		//Add input and output path
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//set output key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		//Map class and Reduce class
		job.setMapperClass(UserReview.Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(PairWritable.class);
		//Adding cache file
		DistributedCache.addCacheFile(new Path(args[2]).toUri(),job.getConfiguration());
		job.setReducerClass(UserReview.Reduce.class);
		//job.setCombinerClass(UserReview.CombineClass.class);
		int returnValue = job.waitForCompletion(true) ? 0 : 1;
		if (job.isSuccessful()) {
			System.out.println("Job was successful");
		} else if (!job.isSuccessful()) {
			System.out.println("Job was not successful");
		}
		return returnValue;
	}

	public static void main(String[] args) throws Exception {
		int exitcode=ToolRunner.run(new UserReview(), args);
		System.exit(exitcode);
	}

}
