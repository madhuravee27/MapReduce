import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordCount {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context outputMapper) throws IOException, InterruptedException{
            String line = value.toString();
            Text keyWord = new Text();
            StringTokenizer stringTokenizer = new StringTokenizer(line);

            while(stringTokenizer.hasMoreTokens()){
                keyWord.set(stringTokenizer.nextToken());
                outputMapper.write(keyWord, new IntWritable(1));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text keyFromMapper, Iterable<IntWritable> valueFromMapper, Context outputReducer) throws IOException, InterruptedException{
            int aggregate = 0;

            for(IntWritable value : valueFromMapper){
                aggregate += value.get();
            }

            outputReducer.write(keyFromMapper, new IntWritable(aggregate));
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();

        Job myJob = new Job(config, "Word Count");

        myJob.setJarByClass(WordCount.class);
        myJob.setMapperClass(MyMapper.class);
        myJob.setReducerClass(MyReducer.class);

        myJob.setOutputFormatClass(TextOutputFormat.class);
        myJob.setInputFormatClass(TextInputFormat.class);

        myJob.setOutputKeyClass(Text.class);
        myJob.setOutputValueClass(IntWritable.class);
        Path outputDir = new Path(args[1]);

        outputDir.getFileSystem(config).delete(outputDir);

        FileInputFormat.addInputPath(myJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(myJob, outputDir);


        System.exit(myJob.waitForCompletion(true)? 0:1);
    }
}
