package WordCountPackage;

import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.*;


public class mult{
        static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
                public void map(LongWritable key, Text value,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                        String [] dall=value.toString().split(":");
                        output.collect(new Text(dall[0]),new Text(dall[1]));
                }
        }

        static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
                public void reduce(Text key, Iterator<Text> values,OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
                        while (values.hasNext()) {
                                output.collect(key, values.next());
                        }
                }
        }


        static class MultiFileOutput extends MultipleTextOutputFormat<Text, Text> {
                protected String generateFileNameForKeyValue(Text key, Text value,String name) {
                        return key.toString();
                }
        }


        public static void main(String[] args) throws Exception {

                Configuration mycon=new Configuration();
                JobConf conf = new JobConf(mycon,mult.class);

                conf.setOutputKeyClass(Text.class);
                conf.setMapOutputKeyClass(Text.class);
                conf.setOutputValueClass(Text.class);

                conf.setMapperClass(Map.class);
                conf.setReducerClass(Reduce.class);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(MultiFileOutput.class);

                FileInputFormat.setInputPaths(conf,new Path("/user/mukundr/MultiFileTest.txt"));
                FileOutputFormat.setOutputPath(conf,new Path("/user/mukundr/NEW"));
                JobClient.runJob(conf);

        }
}
