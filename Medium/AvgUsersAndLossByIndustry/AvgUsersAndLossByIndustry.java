package intermediate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AvgUsersAndLossByIndustry extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/cybersecurity.csv");
        Path output = new Path("output/AvgUsersAndLossByIndustry");

        Job job = Job.getInstance(conf, "AvgUsersAndLossByIndustry");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setMapperClass(AvgUsersAndLossMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IndustryStatsValue.class);

        job.setReducerClass(AvgUsersAndLossReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true)? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(), new AvgUsersAndLossByIndustry(), args);
        System.exit(result);
    }

    public static class AvgUsersAndLossMapper extends Mapper<LongWritable, Text, Text, IndustryStatsValue> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("Country")) {
                return;
            }

            String[] columns = value.toString().split(",");
            String industry = columns[3];
            float financialLoss = Float.parseFloat(columns[4]);
            int affectedUsers = Integer.parseInt(columns[5]);

            context.write(new Text(industry),
                    new IndustryStatsValue(affectedUsers, financialLoss, 1));
        }
    }

    public static class AvgUsersAndLossReducer extends Reducer<Text, IndustryStatsValue, Text, Text> {
        public void reduce(Text key, Iterable<IndustryStatsValue> values, Context context)
                throws IOException, InterruptedException {
            int totalUsers = 0;
            float totalLoss = 0;
            int count = 0;

            for (IndustryStatsValue v : values) {
                totalUsers += v.getAffectedUsers();
                totalLoss += v.getFinancialLoss();
                count += v.getCount();
            }

            float avgUsers = (float) totalUsers / count;
            float avgLoss = totalLoss / count;

            String result = String.format("Avg Users: %.2f, Avg Loss: %.2fM", avgUsers, avgLoss);
            context.write(key, new Text(result));
        }
    }
}