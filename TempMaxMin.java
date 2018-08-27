import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TempMaxMin {
    public static class TempMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{
        public void map(LongWritable key, Text value, Context outputMapper) throws IOException, InterruptedException{
            String[] lineSplit = value.toString().split(" ");
            String date = lineSplit[1];
            String minTemp = lineSplit[5];
            String maxTemp = lineSplit[6];

            float avgTemp = (Float.parseFloat(minTemp) + Float.parseFloat(maxTemp))/2;

            if(avgTemp > 20)
                outputMapper.write(new Text("Hot day"),new FloatWritable(avgTemp));
            else
                outputMapper.write(new Text("Cold day"),new FloatWritable(avgTemp));
        }
    }

    public static class TempReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{
        public void reduce(Text keyFromMapper, Iterable<FloatWritable> valueFromMapper, Context outputReducer) throws IOException, InterruptedException{
            float aggregate = 0;
            int count = 0;

            for(FloatWritable value : valueFromMapper){
                aggregate += value.get();
                count++;
            }

            outputReducer.write(keyFromMapper, new FloatWritable(aggregate/count));
        }
    }

    public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration config = new Configuration();

        Job myJob = new Job(config, "Temp Max Min");

        myJob.setJarByClass(TempMaxMin.class);
        myJob.setMapperClass(TempMapper.class);
        myJob.setReducerClass(TempReducer.class);

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
