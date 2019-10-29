import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
    //   <Input Key Type, Input Value Type, Output Key Type, Output Value Type>
    public static class TextMapper extends Mapper<LongWritable, Text, WordTuple, IntWritable> {

        private final static IntWritable ONE = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            String[] words = line.split("\\W+");
            int numWords = words.length;
            for(int i = 0; i  < numWords; i++)
            {
                String word1 = words[i];
                Text text1 = new Text(word1);
                for(int j = i+1; j < numWords; j++)
                {
                    // boundary condition
                    if(j >= numWords + 1)
                    {
                        continue;
                    }
                    String word2 = words[j];
                    WordTuple tuple = new WordTuple(text1,new Text(word2));
                    context.write(tuple,ONE);
                }
            }

        }
    }

    // Replace "?" with your own key / value types
    // NOTE: combiner's output key / value types have to be the same as those of mapper
    public static class TextCombiner extends Reducer<LongWritable, Text, WordTuple, IntWritable> {
        public void reduce(WordTuple key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable value:values)
            {
                sum += value.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    // Replace "?" with your own input key / value types, i.e., the output
    // key / value types of your mapper function
    public static class TextReducer extends Reducer<WordTuple, IntWritable, Text, Text> {
        private final static Text emptyText = new Text("");

        public void reduce(WordTuple key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
        {
            // Implementation of you reducer function

            int sum = 0;
            for (IntWritable value:values)
            {
                sum += value.get();
            }
            String value = " " + sum;
            context.write(new Text(key.toString()), new Text(value));
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
        job.setMapOutputKeyClass(WordTuple.class);
        job.setMapOutputValueClass(IntWritable.class);

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
        /**
         * TODO: TEST EVERYTHING!!!!
         */
        int res = ToolRunner.run(new Configuration(), new TextAnalyzer(), args);
        System.exit(res);
    }

    // Class to represent a tuple of words
    public static class WordTuple implements WritableComparable<WordTuple>
    {
        private Text word1;
        private Text word2;

        public WordTuple()
        {}

        public WordTuple(Text str1, Text str2)
        {
            this.word1 = str1;
            this.word2 = str2;
        }

        public void setWord1(Text str)
        {
            this.word1 = str;
        }

        public void setWord2(Text str)
        {
            this.word2 = str;
        }


        public Text getWord1()
        {
            return this.word1;
        }

        public Text getWord2()
        {
            return this.word2;
        }

        private boolean compareWords(Text word1,Text word2)
        {
            if(word1 == null)
            {
                return word2 == null;
            }

            return word1.equals(word2);
        }

        @Override
        public void readFields(DataInput in) throws IOException
        {
            if(word1 == null)
            {
                word1 = new Text();
            }
            if(word2 == null)
            {
                word2 = new Text();
            }
            word1.readFields(in);
            word2.readFields(in);
        }

        @Override
        public void write(DataOutput out) throws IOException
        {
            if(word1 == null)
            {
                word1 = new Text();
            }
            if(word2 == null)
            {
                word2 = new Text();
            }
            word1.write(out);
            word2.write(out);
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

        @Override
        public String toString()
        {
            return ""+word1+" "+word2;
        }

        @Override
        public int compareTo(WordTuple other)
        {
            if(other == null)
            {
                return this==null? 0: 1;
            }
            if(word1 != null)
            {
                int comp = word1.compareTo(other.word1);
                if(comp != 0)
                {
                    return comp;
                }
            }
            if(word2 != null)
            {
                return word2.compareTo(other.word2);
            }
            return -1;
        }


    }
}
