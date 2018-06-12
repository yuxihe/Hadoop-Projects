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
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			//pass data to reducer
			String[] lines = value.toString().trim().split("\t");

			context.write(new Text(lines[0]), new Text(lines[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user \t movie:rating
			//pass data to reducer
			String[] lines = value.toString().trim().split("\t");
			String movieId = lines[1].split(":")[0];
			String rating = lines[1].split(":")[1];
			context.write(new Text(movieId), new Text(lines[0] + ":" + rating));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
			HashMap<String, Double> map1 = new HashMap<String, Double>();
			HashMap<String, Double> map2 = new HashMap<String, Double>();

			for(Text value : values) {
				if(value.toString().trim().contains("=")) {
					String[] line = value.toString().trim().split("=");
					map1.put(line[0], Double.parseDouble(line[1]));
				}
				else {
					String[] line = value.toString().trim().split(":");
					map2.put(line[0], Double.parseDouble(line[1]));
				}
			}

			for(Map.Entry<String, Double> entry1 : map1.entrySet()) {
				String movieA = entry1.getKey();
				double relation = entry1.getValue();
				for(Map.Entry<String, Double> entry2 : map2.entrySet()) {
					String userID = entry2.getKey();
					double rating = entry2.getValue();

					double res = rating * relation;
					context.write(new Text(userID + ":" + movieA), new DoubleWritable(res));

				}
			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
