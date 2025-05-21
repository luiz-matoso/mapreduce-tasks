package intermediate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AvgResolutionAndLossByCountryAttack extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Path input = new Path("in/cybersecurity.csv");
        Path output = new Path("output/AvgResolutionAndLossByCountryAttack");

        Job job = Job.getInstance(conf, "AvgResolutionAndLossByCountryAttack");

        FileInputFormat.addInputPath(job, input);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);

        job.setJarByClass(AvgResolutionAndLossByCountryAttack.class);

        job.setMapperClass(AttackStatsMapper.class);
        job.setMapOutputKeyClass(CountryAttackKey.class);
        job.setMapOutputValueClass(AttackStatsValue.class);

        job.setReducerClass(AttackStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        int result = ToolRunner.run(new Configuration(),
                new AvgResolutionAndLossByCountryAttack(), args);
        System.exit(result);
    }

    public static class AttackStatsMapper extends Mapper<LongWritable, Text, CountryAttackKey, AttackStatsValue> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("Country")) {
                return;
            }

            String[] columns = value.toString().split(",");

            if (columns.length < 10) {
                return;
            }

            String country = columns[0].trim();
            String attackType = columns[2].trim();

            try {
                float financialLoss = Float.parseFloat(columns[4].trim());
                int resolutionTime = Integer.parseInt(columns[9].trim());

                CountryAttackKey outputKey = new CountryAttackKey(country, attackType);
                AttackStatsValue outputValue = new AttackStatsValue(financialLoss, resolutionTime, 1);

                context.write(outputKey, outputValue);
            } catch (NumberFormatException e) {
                // Ignora registros mal formatados
            }
        }
    }

    public static class AttackStatsReducer extends Reducer<CountryAttackKey, AttackStatsValue, Text, Text> {
        public void reduce(CountryAttackKey key, Iterable<AttackStatsValue> values, Context context)
                throws IOException, InterruptedException {
            float totalLoss = 0;
            int totalResolutionTime = 0;
            int totalCount = 0;

            for (AttackStatsValue v : values) {
                totalLoss += v.getFinancialLoss();
                totalResolutionTime += v.getResolutionTime();
                totalCount += v.getCount();
            }

            float avgResolution = (float) totalResolutionTime / totalCount;

            String result = String.format(
                    "Avg Resolution Time: %.2f hours, Total Loss: $%.2fM, Total Incidents: %d",
                    avgResolution, totalLoss, totalCount
            );

            context.write(new Text(key.toString()), new Text(result));
        }
    }
}
