import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopFriend {
	//class for Map 
		public static class Map
		extends Mapper<LongWritable, Text, Text, Text>{
			Text user=new Text();
			Text friendlist=new Text();
			public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
				//splitting friends and user
				String[] splitinp=value.toString().split("\\t");
				String userid=splitinp[0];
				if(splitinp.length==1){
					return;
				}
				String[] friends=splitinp[1].split(",");			
				for(String friend : friends){
					if(userid.equals(friend))
						continue;
					String userkey=(Integer.parseInt(friend)>Integer.parseInt(userid))?userid+","+friend:friend+","+userid;
					String regex="((\\b" +friend+",+\\b)|(\\b+,"+friend +"\\b))";
					String uservalue=splitinp[1].replaceAll(regex, "");
					user.set(userkey);
					friendlist.set(uservalue);
					context.write(user, friendlist);
				}
			}
		}
		//class for Reduce
		public static class Reduce
		extends Reducer<Text,Text,Text,Text>{
			Text result=new Text();
			Text key=new Text();
			//hashmap to store the count of mutual friends
			LinkedHashMap<String,Integer> map=new LinkedHashMap<String,Integer>();
			//hashmap to store the user and commonfriends
			LinkedHashMap<String,String> mutualmap=new LinkedHashMap<String,String>();
			//function to find common friends by removing values that are unique in each group
			public String matchlist(String friendlist1,String friendlist2){
				//splitting the first list to seperate string values with comma as delimiter
				String[] templist1=friendlist1.split(",");
				String[] templist2=friendlist2.split(",");
				//Creating an Arraylist to store common values in two strings
				ArrayList<String> friendsgroup1=new ArrayList<String>();
				ArrayList<String> friendsgroup2=new ArrayList<String>();
				for(String value:templist1){
					friendsgroup1.add(value);
				}
				for(String value:templist2){
					friendsgroup2.add(value);
				}
				friendsgroup1.retainAll(friendsgroup2);
				return friendsgroup1.toString().replaceAll("\\[|\\]","");
			}
			//Reduce function
			public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
				String[] friendlist=new String[2];
				int counter=0;
				for(Text value:values){
					friendlist[counter]=value.toString();
					counter=counter+1;
				}
				String commonfriends=matchlist(friendlist[0],friendlist[1]);
				map.put(key.toString(),commonfriends.split(",").length);
				mutualmap.put(key.toString(),commonfriends);
			}
			public void cleanup(Context context)throws IOException,InterruptedException{
				map=sortByValue(map);
				int counter=0;
				for(HashMap.Entry<String,Integer> entry:map.entrySet()){
					if(counter==12)break;
					key.set(entry.getKey());
					result.set(entry.getValue().toString());
					context.write(key, result);
					counter=counter+1;
					}
			}
			public LinkedHashMap<String,Integer> sortByValue(HashMap<String,Integer> countmap){
				//convert map to a list of map
				List<HashMap.Entry<String,Integer>> list=new LinkedList<HashMap.Entry<String,Integer>>(countmap.entrySet());
				Collections.sort(list, new Comparator<HashMap.Entry<String, Integer>>() {
		            public int compare(HashMap.Entry<String, Integer> o1,HashMap.Entry<String, Integer> o2) {
		            		return -(o1.getValue().compareTo(o2.getValue()));
		            }
				});
				//Adding the sorted value to linkedhashmap
				LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		        for (HashMap.Entry<String, Integer> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		        }
		        return sortedMap;
			}
		}

	public static void main(String[] args) throws Exception {
		Configuration conf=new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
		/* get all args*/
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <FriendsFile> <output>");
            System.exit(2);
        }
        //create a job with name mutual friend
        Job job=new Job(conf,"TopFriend");
        job.setJarByClass(TopFriend.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set hdfs path of input data
        FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
        //set HDFS path for output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
      	System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
