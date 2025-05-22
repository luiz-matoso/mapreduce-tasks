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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AvgResolutionByDefenseAndSource extends Configured implements Tool {

  public static class ResolutionMapper extends Mapper<Object, Text, DefenseSourceKey, TimeStatsValue> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Ignora o cabeçalho
      if (((LongWritable) key).get() == 0 && value.toString().contains("Country"))
        return;

      String[] cols = value.toString().split(",");
      if (cols.length < 10)
        return;

      String defense = cols[8].trim();
      String source = cols[6].trim();

      int resolutionTime;
      try {
        resolutionTime = Integer.parseInt(cols[9].trim());
      } catch (NumberFormatException e) {
        return;
      }

      context.write(new DefenseSourceKey(defense, source), new TimeStatsValue(resolutionTime, 1));
    }
  }

  public static class ResolutionCombiner
      extends Reducer<DefenseSourceKey, TimeStatsValue, DefenseSourceKey, TimeStatsValue> {
    public void reduce(DefenseSourceKey key, Iterable<TimeStatsValue> values, Context context)
        throws IOException, InterruptedException {
      int sumTime = 0;
      int count = 0;

      for (TimeStatsValue v : values) {
        sumTime += v.getTime();
        count += v.getCount();
      }

      context.write(key, new TimeStatsValue(sumTime, count));
    }
  }

  public static class ResolutionReducer extends Reducer<DefenseSourceKey, TimeStatsValue, Text, Text> {
    public void reduce(DefenseSourceKey key, Iterable<TimeStatsValue> values, Context context)
        throws IOException, InterruptedException {
      int sumTime = 0;
      int count = 0;

      for (TimeStatsValue v : values) {
        sumTime += v.getTime();
        count += v.getCount();
      }

      float avg = (float) sumTime / count;
      context.write(new Text(key.toString()), new Text(String.format("Avg Time: %.2f h | Incidents: %d", avg, count)));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Path input = new Path("in/cybersecurity.csv");
    Path output = new Path("output/AvgResolutionByDefenseAndSource");

    FileSystem.get(conf).delete(output, true);

    Job job = Job.getInstance(conf, "ResolutionByDefenseAndSource");

    job.setJarByClass(AvgResolutionByDefenseAndSource.class);
    job.setMapperClass(ResolutionMapper.class);
    job.setCombinerClass(ResolutionCombiner.class);
    job.setReducerClass(ResolutionReducer.class);

    job.setMapOutputKeyClass(DefenseSourceKey.class);
    job.setMapOutputValueClass(TimeStatsValue.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    int res = ToolRunner.run(new Configuration(), new AvgResolutionByDefenseAndSource(), args);
    System.exit(res);
  }
}
