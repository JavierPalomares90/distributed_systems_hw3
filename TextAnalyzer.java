import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

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
    public static class TextMapper extends Mapper<LongWritable, Text, WordTuple, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] words = line.split("\\s");
            int numWords = words.length;
            for(int i = 0; i  < numWords; i++)
            {
                String word1 = words[i];
                for(int j = i+1; j < numWords; j++)
                {
                    String word2 = words[j];
                    WordTuple tuple = new WordTuple(word1,word2);
                    // TODO: Write the tuple and one

                }
            }

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

    // Do not modify the main method
    public static void main(String[] args) throws Exception 
    {
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }


     // Class to represent the nodes in the graph. Each node represent a word in the file
    public static class Node
    {
        private String word;

        public Node(String word)
        {
            this.setWord(word);
        }

        public Node()
        {
        }

        public void setWord(String word)
        {
            this.word = word;
        }

        public String getWord()
        {
            return this.word;
        }
    }

    // Class to represent a tuple of words
    public static class WordTuple
    {
        private String word1;
        private String word2;

        public WordTuple()
        {}

        public WordTuple(String str1, String str2)
        {
            this.word1 = str1;
            this.word2 = str2;
        }

        public void setWord1(String str)
        {
            this.word1 = str;
        }

        public void setWord2(String str)
        {
            this.word2 = str;
        }


        public String getWord1()
        {
            return this.word1;
        }

        public String getWord2()
        {
            return this.word2;
        }

        private boolean compareWords(String word1,String word2)
        {
            if(word1 == null)
            {
                return word2 == null;
            }

            return word1.equals(word2);
        }

        @Override
        public boolean equals(Object o)
        {

            // If the object is compared with itself then return true   
            if (o == this) 
            {
                return true; 
            } 
  
            if ((o instanceof WordTuple) == false) 
            {
                return false; 
            } 

            if(this == null || o == null)
            {
                return  this == o;
            }

            WordTuple other = (WordTuple) o;

            boolean b1 = compareWords(this.word1, other.getWord1());
            boolean b2 = compareWords(this.word2, other.getWord2());

            if(b1 == true && b2 == true)
            {
                return true;
            }
            boolean b3 = compareWords(this.word1, other.getWord2());
            boolean b4 = compareWords(this.word2, other.getWord1());
            if(b3 == true && b4 == true)
            {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            int result = 17;
            if(word1 != null)
            {
                result = 31 * result + word1.hashCode();
            }
            if(word2 != null)
            {
                result = 31 * result + word2.hashCode();
            }

            return result;

        }


    }

    // Class to represent the edges in the graph.
    public static class Edge
    {
        private Node u;
        private Node v;
        private int weight;

        public Edge(Node n1,Node n2, int weight)
        {
            this.u = n1;
            this.v = n2;
            this.weight = weight;
        }

        public int getWeight()
        {
            return this.weight;
        }

        public void setWeight(int weight)
        {
            this.weight = weight;
        }

    }
}