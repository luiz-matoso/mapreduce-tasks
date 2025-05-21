package basic;

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
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AffectedUsersPerYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        Path input = new Path("in/cybersecurity.csv");
        Path output = new Path("output/AffectedUsersPerYear");

        Job j = new Job(c, "maxusersaffected");
        j.setJarByClass(AffectedUsersPerYear.class);
        j.setMapperClass(MapForAffectedUsers.class);
        j.setReducerClass(ReduceForAffectedUsers.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAffectedUsers extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text year = new Text();
        private IntWritable users = new IntWritable();

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] cols = line.split(",");

            if (cols.length > 5 && !line.contains("Country")) {
                year.set(cols[1].trim());
                users.set(Integer.parseInt(cols[5].trim()));
                con.write(year, users);
            }
        }
    }

    public static class ReduceForAffectedUsers extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int max = 0;
            for (IntWritable val : values) {
                if (val.get() > max) {
                    max = val.get();
                }
            }
            result.set(max);
            con.write(key, result);
        }
    }
}