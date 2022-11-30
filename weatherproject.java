import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

#these packages are present in hadoop-common.jar
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;

#present in haddop map reduce core
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Weather

public static class Map extends Mapper<LongWritable, Text, Text, Text>
{
    private String current_filename = "";

    protected void setup(Context context)
    {
        // get the name of the current to-be-read file
        InputSplit split = context.getInputSplit();
        Path path = ((FileSplit) split).getPath();
        current_filename = path.getName();
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        if(current_filename.equals("WeatherStationLocations.csv"))    // if mapper is reading through the weather file
        {
            if(value.toString().contains("USAF"))   // remove header
                return;
            else
            {
                String[] columns = value.toString().split(", "); // splitting columns
                String id = columns[0]; //filtering the columns I want  
                String id = columns[1];
                String id = columns[2];
                String id = columns[3];
        

       
                context.write(new Text(columns[0]), new Text(columns[1]));
            }
        }
    
            }
        }


        //second mapper to read txt files
        public static class Map extends Mapper<LongWritable, Text, Text, Text>
        {
            private String current_filename = "";
        
            protected void setup(Context context)
            {
                // get the name of the current to-be-read file
                InputSplit split = context.getInputSplit();
                Path path = ((FileSplit) split).getPath();
                current_filename = path.getName();
            }
        
            public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
            {
                if(current_filename.indexOf(".txt"))    // mapper checking last 4 characters of our text files so we dont need to have a mapper for each txt file
                {
                    if(value.toString().contains("STN"))   // remove header
                        return;
                    else
                    {
                        String[] columns = value.toString().split("\t\t");  // 2 tabs as delimiter
                        String id = columns[0]; //filtering the columns I want  
                        String id = columns[1];
                        String id = columns[2];
                        String id = columns[3];
        
               
                        context.write(new Text(columns[0]), new Text(columns[1]));
                    }
                }
            
                    }
                }    

//reducer function 
public static class Reduce extends Reducer<Text, Text, Text, Text>
  { 
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
                 {
                    List<String> customer_records = new ArrayList<String>();
            
                     // put all the values in a list to find the size of them
                     for(Text value : values)
                         customer_records.add(value.toString());
            
                     // if there's only one record, i.e. just the ID and the customer's name in they key-value pairs,
                    // write their ID and name to output
                    if(customer_records.size() == 1)
                            context.write(key, new Text(customer_records.get(0)));
                    }
                }

// run the map reduce 
public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("input");
        Path output_dir = new Path("output");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job job = Job.getInstance(conf, "Weather");
        job.setJarByClass(OrderListFilter.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input_dir);
        FileOutputFormat.setOutputPath(job, output_dir);
        job.waitForCompletion(true);
    }
}