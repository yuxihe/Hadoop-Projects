import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AverageRating {

    public static class AverageRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input user,movie,rating
            //divide data by user
            String[] user_movie_rating = value.toString().trim().split(",");
            String userID = user_movie_rating[0];
            String movieId = user_movie_rating[1];
            String rating = user_movie_rating[2];

            context.write(new Text(userID), new Text(movieId + ":" + rating));
        }
    }

    public static class AverageRatingReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA, value=<movieB:relation, movieC:relation...>
            //normalize each unit of co-occurrence matrix
            int sum = 0;
            int count = 0;
            for(Text value : values) {
                String[] lines = value.toString().trim().split(":");
                double rating = Double.parseDouble(lines[1]);
                sum += rating;
                count++;
            }
            double outputValue = (double) sum/count;
            context.write(key, new Text(String.valueOf(outputValue)));

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(AverageRatingMapper.class);
        job.setReducerClass(AverageRatingReducer.class);

        job.setJarByClass(AverageRating.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

