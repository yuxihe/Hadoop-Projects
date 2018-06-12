import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class preProcessRating {
    public static class AverageRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //input: userId \t averageRating
            //pass data to reducer
            String[] lines = value.toString().trim().split("\t");

            context.write(new Text(lines[0]), new Text(lines[1]));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //input: user,movie,rating
            //pass data to reducer
            String[] lines = value.toString().trim().split(",");
            context.write(new Text(lines[0]), new Text(lines[1] + ":" + lines[2]));
        }
    }

    public static class preProcessRatingReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<String>();
            String average = "";
            for(Text value : values) {
                if(value.toString().contains(":")) {
                    list.add(value.toString());
                }
                else {
                    average = value.toString();
                }
            }

            for(String str : list) {
                String[] lines = str.split(":");
                if(Double.parseDouble(lines[1]) == 0) {
                    context.write(key, new Text(lines[0] + ":" + average));
                }
                else {
                    context.write(key, new Text(str));
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(preProcessRating.class);

        ChainMapper.addMapper(job, AverageRatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

        job.setMapperClass(AverageRatingMapper.class);
        job.setMapperClass(RatingMapper.class);

        job.setReducerClass(preProcessRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AverageRatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
