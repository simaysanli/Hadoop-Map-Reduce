package MRMean;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MeanOperation {
	public static void main(String[] args) throws Exception {
		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);

		@SuppressWarnings("deprecation")
		Job j = new Job(c, "wordcount");
		j.setJarByClass(MeanOperation.class);
		j.setMapperClass(MapForSumOperation.class);
		j.setReducerClass(ReduceForSumOperation.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForSumOperation extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final int numOfCols = 109;
		private final int yearIdx = 2, sexIdx = 3, agesStartIdx = 5;

		private String[] lineToList(String line, int num_of_cols) {
			String[] list = new String[num_of_cols];

			line = line.substring(0, line.length() - 1);
			list = line.split(",");

			return list;
		}

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {

			String[] rowList = lineToList(value.toString(), numOfCols);
//			int line_year = Integer.parseInt(rowList[yearIdx]);

//			System.out.println(line_year + "," + line_sex);
//			System.out.println(input_minYear + "," + input_maxYear + "," + input_sex);

			for(int input_age=0;input_age <=100;input_age++) {
				String formattedAge = String.format("%03d", input_age);
				Text outputKey = new Text(rowList[yearIdx] + " " + formattedAge);
				IntWritable outputValue = new IntWritable(Integer.parseInt(rowList[agesStartIdx + input_age]));
				con.write(outputKey, outputValue);
				System.out.println(">>>>>>" +outputKey + "," + outputValue + "written!");
			}
		}

	}

	public static class ReduceForSumOperation extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text year, Iterable<IntWritable> values, Context con)
				throws IOException, InterruptedException {
			int sum = 0;
			int count=0;
			int mean=0;
			for (IntWritable value : values) {
				sum += value.get();
				count+=1;
			}
			mean = sum/count;
			con.write(year, new IntWritable(mean));
		}
	}
}
