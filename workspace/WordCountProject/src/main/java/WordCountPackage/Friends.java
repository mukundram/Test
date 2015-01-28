package WordCountPackage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Friends{
	public static void main(String[] args) throws Exception
	{
		Job job = new Job();
		job.setJarByClass(Friends.class);
		job.setJobName("HadoopTest");
		FileInputFormat.addInputPath(job, new Path("/user/mukundr/Friendship.txt"));
		FileOutputFormat.setOutputPath(job, new Path("/user/mukundr/FriendOutput"));
		
		job.setMapperClass(FriendMapper.class);
		job.setReducerClass(FriendReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0:1);		

	}	
	static class FriendMapper extends Mapper <LongWritable, Text, Text, TextArrayWritable>
	{		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{		
			String [] friends_array = value.toString().split(",");			
			String [] friends_array_rem = new String[friends_array.length-1];
			
			for (int i = 1; i<friends_array.length;i++)
			{
				friends_array_rem[i-1]=friends_array[i];
			}			
			
			List<String> friends_list = Arrays.asList(friends_array);			
			String key_1 = friends_list.get(0);			
			
			for (int i = 1; i<friends_list.size();i++)
			{				
				List<String> key_all = new ArrayList<String>();
				key_all.add(key_1);
				String key_2 = friends_list.get(i);
			    key_all.add(key_2);				
				Collections.sort(key_all);
				context.write(new Text(key_all.toString()), new TextArrayWritable(friends_array_rem));
			}			
		}
	}	
	static class FriendReducer extends Reducer<Text, TextArrayWritable, Text, Text>
	{
		public void reduce(Text key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException
		{			
			ArrayList<ArrayList<String>> temp = new ArrayList<ArrayList<String>> ();			
			for (TextArrayWritable value:values)
			 {
				ArrayList<String> temp1 = new ArrayList<String>();		 
				for( Text text : value.get() )
				{
					temp1.add(text.toString());
				}
				temp.add(temp1);
			  }
			
			TreeSet<String> tempSet1 = new TreeSet<String>(temp.get(0));
			tempSet1.retainAll(temp.get(1));
			context.write(key, new Text(tempSet1.toString()));
		}
	}
	public static class TextArrayWritable extends ArrayWritable 
	{
	public TextArrayWritable() 
	{
    super(Text.class);
    }
    public TextArrayWritable(String[] strings) 
    {
       super(Text.class);
       Text[] texts = new Text[strings.length];
       for (int i = 0; i < strings.length; i++) 
       {
           texts[i] = new Text(strings[i]);
       }
        set(texts);
	}
    public Text[] get() {
        Writable[] writables = super.get();
        Text[] texts = new Text[writables.length];
        for(int i=0; i<writables.length; ++i)
           texts[i] = (Text)writables[i];
        return texts;
     }
	
    
    @Override
    public String toString() {   	
    	
    	StringBuilder sb = new StringBuilder();
        for (String s : super.toStrings())
        {
            sb.append(s).append(" ");
        }
        return sb.toString();
    }    
	}
}
