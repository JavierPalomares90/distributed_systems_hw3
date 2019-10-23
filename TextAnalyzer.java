import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TextAnalyzer extends Configured implements Tool
{
    // Replace "?" with your own output key / value types
    // The four template data types are:
    /**
     * TODO: Check these output types. They may not be correct. 
     * For now, set them to Text just for compiliation purposes
     */
    //   <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you mapper function
        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<LongWritable, Text, Text, Text> {
        public void reduce(Text key, Iterable<TupleWritable> tuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you combiner function
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<Text, Text, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(Text key, Iterable<TupleWritable> queryTuples, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function
           Map<String,Integer> map = null;

            // Write out the results; you may change the following example
            // code to fit with your reducer function.
            //   Write out each edge and its weight
            Text value = new Text();
            for(String neighbor: map.keySet()){
                String weight = map.get(neighbor).toString();
                value.set(" " + neighbor + " " + weight);
                context.write(key, value);
            }
            //   Empty line for ending the current context key
            context.write(emptyText, emptyText);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "jp46396_eid2"); // TODO: Replace with Matt's EID
        job.setJarByClass(TextAnalyzer.class);

        // Setup MapReduce job
        job.setMapperClass(TextMapper.class);

        // set local combiner class
        job.setCombinerClass(TextCombiner.class);

        // set reducer class
        job.setReducerClass(TextReducer.class);

        // Specify key / value types (Don't change them for the purpose of this assignment)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //   If your mapper and combiner's  output types are different from Text.class,
        //   then uncomment the following lines to specify the data types.
        //job.setMapOutputKeyClass(?.class);
        //job.setMapOutputValueClass(?.class);

        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    -    // Do not modify the main method
    public static void main(String[] args) throws Exception 
    {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }
}