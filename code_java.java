import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;


public class Crypto {
	public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		private HashMap<Text, Integer> records = new HashMap<Text, Integer>();
		@Override
		public void map (LongWritable offset, Text csv, Context context) throws IOException, InterruptedException{
			String[] tokens = csv.toString().split(",");

			if (tokens[1].equals("\"1h\"")) {
				Text coin = new Text(tokens[0].substring(1, tokens[0].length() - 1)); // remove double quotes
				records.put(coin, records.get(coin) == null ? 0 : records.get(coin) + 1);

				if (records.get(coin) < 100) // Period of 100 intervals
					context.write(coin, new DoubleWritable(Double.parseDouble(tokens[6])));
			}
		}
	}
	public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
		@Override
		public void reduce(Text coin, Iterable<DoubleWritable> prixs, Context context) throws IOException, InterruptedException{
			double sommesqr=0.0;
			double somme=0.0;
			double length = 0.0;
			double variance=0.0;
			double ecartype=0.0;
			for (DoubleWritable prix : prixs) {
				sommesqr += Math.pow(prix.get(), 2);
				somme += prix.get();
				length++;
			}
			variance = (sommesqr - Math.pow(somme, 2)/length)/(length-1);
			ecartype = Math.sqrt(variance);
			context.write(coin, new DoubleWritable(ecartype));
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "crypto");
		job.setJarByClass(Crypto.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
