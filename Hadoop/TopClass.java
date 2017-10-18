import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.WritableComparable;
//class for mapreduce
public class TopClass extends Configured implements Tool{
	//class for map to get business id and stars from review
	public static class Map extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			String[] businessdata=value.toString().split("::");
			context.write(new Text(businessdata[2]),new Text(businessdata[3]));
		}
	}
	//class for reduce to compute average stars for a business
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		Text outkey=new Text();
		Text average=new Text();
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			int totalcategories=0;
			float sum=0;
			for(Text value:values){
				sum=sum+Float.parseFloat(value.toString());
				totalcategories+=1;
			}
			float avg=new Float(sum/totalcategories);
			outkey.set(key);
			average.set(Float.toString(avg));
			context.write(outkey, average);
		}
	}
	//class for partition
	public static class MyPartition extends Partitioner<PairKey,Text>{
		public int getPartition(PairKey key,Text value,int numPartitions){
			return key.getId().hashCode()%numPartitions;
		}
	}
	//class for business map
	public static class BusinessMap extends Mapper<LongWritable,Text,PairKey,Text>{
		PairKey interkey=new PairKey();
		Text result=new Text();
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
			//splitting business.csv file
			String[] business=value.toString().split("::");
			interkey.set(business[0],1);
			result.set(business[1]+"\t"+business[2]);
			context.write(interkey, result);
		}
	}
	//class for review map
	public static class ReviewMap extends Mapper<Text,Text,PairKey,Text>{
		PairKey interkey=new PairKey();
		Text result=new Text();
		public void map(Text key,Text value,Context context)throws IOException,InterruptedException{
			//Taking the output of first job business id and rating and output intermediate key value pair
			interkey.set(key.toString(),2);
			result.set(value);
			context.write(interkey, result);
		}
	}
	//class for Final reducer
	public static class FinalReducer extends Reducer<PairKey,Text,Text,Text>{
		private StringBuilder builder=new StringBuilder();
		//Hashmap to store actual result to be displayed and other one to store id and rating for top ten computation
		LinkedHashMap<String,String> res=new LinkedHashMap<String,String>();
		LinkedHashMap<String,Float> comp=new LinkedHashMap<String,Float>();
		//key and val for final output
		Text keyval=new Text();
		Text result=new Text();
		public void reduce(PairKey key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
			String bid=key.getId().toString();
			Iterator<Text> inter=values.iterator();
			Text value=inter.next();
				if(res.containsKey(bid)!=false){
					res.put(bid,res.get(bid)+"\t"+value.toString());
					comp.put(bid,Float.parseFloat(value.toString()));
				}
				else
					res.put(bid,value.toString());
				
			}
		
		public void cleanup(Context context)throws IOException,InterruptedException{
			comp=sortByValue(comp);
			int counter=0;
			for(HashMap.Entry<String,Float> entry:comp.entrySet()){
				if(counter==10)break;
				keyval.set(entry.getKey().toString());
				result.set(res.get(entry.getKey()));
				context.write(keyval, result);
				counter=counter+1;
			}
		}
		public LinkedHashMap<String,Float> sortByValue(HashMap<String,Float> countmap){
			//convert map to a list of map
			List<HashMap.Entry<String,Float>> list=new LinkedList<HashMap.Entry<String,Float>>(countmap.entrySet());
			Collections.sort(list, new Comparator<HashMap.Entry<String, Float>>() {
	            public int compare(HashMap.Entry<String,Float> o1,HashMap.Entry<String,Float> o2) {
	            		return -(o1.getValue().compareTo(o2.getValue()));
	            }
			});
			//Adding the sorted value to linkedhashmap
			LinkedHashMap<String, Float> sortedMap = new LinkedHashMap<String,Float>();
	        for (HashMap.Entry<String,Float> entry : list) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }
	        return sortedMap;
		}
	}
	//Run tool
	public int run(String[] args)throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"TopClass");
		job.setJarByClass(TopClass.class);
		//taking input and output path from command line
		Path in=new Path(args[0]);
		Path out=new Path(args[1]);
		//Input
		FileInputFormat.setInputPaths(job,in);
		FileOutputFormat.setOutputPath(job,out);
		//set the number of reduce task which can be given from commnad line also since using tool interface
		job.setNumReduceTasks(1);
		//setting the mapper and reduce class
		job.setMapperClass(TopClass.Map.class);
		job.setReducerClass(TopClass.Reduce.class);
		//setting output key call and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		//second job
		Configuration conf2=getConf();
		Job job2=Job.getInstance(conf2,"join");
		job2.setJarByClass(TopClass.class);
		//Taking path for second input and final output
		Path secondInput=new Path(args[2]);
		Path finaloutput=new Path(args[3]);
		//Taking input for Mapper class for business.csv and review.csv
		MultipleInputs.addInputPath(job2,secondInput,TextInputFormat.class,TopClass.BusinessMap.class);
		MultipleInputs.addInputPath(job2,out,KeyValueTextInputFormat.class,TopClass.ReviewMap.class);
		job2.setPartitionerClass(TopClass.MyPartition.class);
		job2.setReducerClass(TopClass.FinalReducer.class);
		//final output
		FileOutputFormat.setOutputPath(job2,finaloutput);
		//setting map output key and value
		job2.setMapOutputKeyClass(PairKey.class);
		job2.setMapOutputValueClass(Text.class);
		//setting output key and value class and output value class
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		System.exit(job2.waitForCompletion(true)?0:1);
		return 0;	
	}
	public static void main(String[] args) throws Exception {
	//Toolrunner to execute mapreduce job in its static method 'run
		int res=ToolRunner.run(new Configuration(),new TopClass(), args);
		System.exit(res);
	}
}
