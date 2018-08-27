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

public class MovieRating {
    //Stage 1 - part 1 - Mapper - Output: MovieId, rating
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        public void map(LongWritable key, Text value, Context mapperOutput) throws IOException, InterruptedException{
            String[] lineSplit = value.toString().split("::");
            int movieId = Integer.parseInt(lineSplit[0]);
            int rating = Integer.parseInt(lineSplit[2]);

            mapperOutput.write(new IntWritable(movieId), new IntWritable(rating));
        }
    }

    //Stage 1 - part 1 - Reducer - Output: MovieId, average rating
    public static class RatingReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>{
        public void reduce(IntWritable movieId, Iterable<IntWritable> ratings, Context reducerOutput) throws IOException, InterruptedException{
            float avgRating = 0;
            int count = 0;

            for(IntWritable rating : ratings){
                avgRating += rating.get();
                count++;
            }
            avgRating /= count;

            reducerOutput.write(movieId, new FloatWritable(avgRating));
        }
    }

    //Stage 1 - part 2 - Reducer - Output: MovieId, Movie name
    public static class MovieMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        public void map(LongWritable key, Text value, Context mapper2Output) throws IOException, InterruptedException{
            String[] lineSplit = value.toString().split("::");
            int movieId = Integer.parseInt(lineSplit[0]);
            String movieName = lineSplit[1];

            mapper2Output.write(new IntWritable(movieId), new Text("name:" + movieName));
        }
    }

    //Stage 2 - part 1 - Mapper - Output: MovieId, average rating in string
    public static class RatingMovieMapper extends Mapper<IntWritable, FloatWritable, IntWritable, Text>{
        public void map(IntWritable key, FloatWritable value, Context mapper3Output) throws IOException, InterruptedException{
            mapper3Output.write(key, new Text("avgRating:" + String.valueOf(value.get())));
        }
    }

    //Stage 2 - part 1 - Reducer - Output: Movie name, average rating
    public static class RatingMovieReducer extends Reducer<IntWritable, Text, Text, FloatWritable>{
        public void reduce(IntWritable movieId, Iterable<Text> values, Context reducer2Output) throws IOException, InterruptedException{
            String name = "";
            float avgRating = 0;
            for(Text t : values){
                String []split = (t.toString()).split(":");
                if(split[0].equals("name")){
                    name = split[1];
                }
                else{
                    avgRating = Float.parseFloat(split[1]);
                }
            }
            reducer2Output.write(new Text(name), new FloatWritable(avgRating));
        }
    }
}
