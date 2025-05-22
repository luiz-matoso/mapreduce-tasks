package advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AttackFinancialLossByCountry extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    int exitCode = ToolRunner.run(new AttackFinancialLossByCountry(), args);
    System.exit(exitCode);
  }

  // ========== JOB 1: Calcular perda financeira média por (Attack Type, Country)
  // ==========

  public static class FinancialLossMapper extends Mapper<LongWritable, Text, AttackCountryKey, FinancialLossValue> {
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      if (key.get() == 0 && value.toString().contains("Country")) {
        return; // Pular cabeçalho
      }

      String[] columns = value.toString().split(",");
      String country = columns[0];
      String attackType = columns[2];
      double financialLoss = Double.parseDouble(columns[4]);

      AttackCountryKey outKey = new AttackCountryKey(attackType, country);
      FinancialLossValue outValue = new FinancialLossValue(financialLoss, 1);

      context.write(outKey, outValue);
    }
  }

  public static class FinancialLossCombiner
      extends Reducer<AttackCountryKey, FinancialLossValue, AttackCountryKey, FinancialLossValue> {
    public void reduce(AttackCountryKey key, Iterable<FinancialLossValue> values, Context context)
        throws IOException, InterruptedException {
      double totalLoss = 0;
      int count = 0;

      for (FinancialLossValue v : values) {
        totalLoss += v.getFinancialLoss();
        count += v.getCount();
      }

      context.write(key, new FinancialLossValue(totalLoss, count));
    }
  }

  public static class FinancialLossReducer
      extends Reducer<AttackCountryKey, FinancialLossValue, AttackCountryKey, DoubleWritable> {
    public void reduce(AttackCountryKey key, Iterable<FinancialLossValue> values, Context context)
        throws IOException, InterruptedException {
      double totalLoss = 0;
      int count = 0;

      for (FinancialLossValue v : values) {
        totalLoss += v.getFinancialLoss();
        count += v.getCount();
      }

      double avgLoss = totalLoss / count;
      context.write(key, new DoubleWritable(avgLoss));
    }
  }

  // ========== JOB 2: Rankear países com maior perda financeira por tipo de
  // ataque ==========

  public static class TopCountriesMapper extends Mapper<AttackCountryKey, DoubleWritable, Text, CountryLossValue> {
    public void map(AttackCountryKey key, DoubleWritable value, Context context)
        throws IOException, InterruptedException {
      context.write(new Text(key.getAttackType()),
          new CountryLossValue(key.getCountry(), value.get()));
    }
  }

  public static class TopCountriesReducer extends Reducer<Text, CountryLossValue, Text, Text> {
    public void reduce(Text key, Iterable<CountryLossValue> values, Context context)
        throws IOException, InterruptedException {
      String topCountry = null;
      double maxLoss = Double.MIN_VALUE;

      for (CountryLossValue v : values) {
        if (v.getFinancialLoss() > maxLoss) {
          maxLoss = v.getFinancialLoss();
          topCountry = v.getCountry();
        }
      }

      if (topCountry != null) {
        String result = String.format("Top Country: %s (Avg Loss: $%.2fM)", topCountry, maxLoss);
        context.write(key, new Text(result));
      }
    }
  }

  // ========== Classes Customizadas ==========

  public static class AttackCountryKey implements WritableComparable<AttackCountryKey> {
    private Text attackType;
    private Text country;

    public AttackCountryKey() {
      this.attackType = new Text();
      this.country = new Text();
    }

    public AttackCountryKey(String attackType, String country) {
      this.attackType = new Text(attackType);
      this.country = new Text(country);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      attackType.write(out);
      country.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      attackType.readFields(in);
      country.readFields(in);
    }

    @Override
    public int compareTo(AttackCountryKey o) {
      int cmp = attackType.compareTo(o.attackType);
      if (cmp != 0)
        return cmp;
      return country.compareTo(o.country);
    }

    public String getAttackType() {
      return attackType.toString();
    }

    public String getCountry() {
      return country.toString();
    }

    @Override
    public String toString() {
      return attackType + "," + country;
    }
  }

  public static class FinancialLossValue implements Writable {
    private DoubleWritable financialLoss;
    private IntWritable count;

    public FinancialLossValue() {
      this.financialLoss = new DoubleWritable();
      this.count = new IntWritable();
    }

    public FinancialLossValue(double financialLoss, int count) {
      this.financialLoss = new DoubleWritable(financialLoss);
      this.count = new IntWritable(count);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      financialLoss.write(out);
      count.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      financialLoss.readFields(in);
      count.readFields(in);
    }

    public double getFinancialLoss() {
      return financialLoss.get();
    }

    public int getCount() {
      return count.get();
    }
  }

  public static class CountryLossValue implements Writable {
    private Text country;
    private DoubleWritable financialLoss;

    public CountryLossValue() {
      this.country = new Text();
      this.financialLoss = new DoubleWritable();
    }

    public CountryLossValue(String country, double financialLoss) {
      this.country = new Text(country);
      this.financialLoss = new DoubleWritable(financialLoss);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      country.write(out);
      financialLoss.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      country.readFields(in);
      financialLoss.readFields(in);
    }

    public String getCountry() {
      return country.toString();
    }

    public double getFinancialLoss() {
      return financialLoss.get();
    }
  }

  // ========== Driver Code ==========

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Path input = new Path("in/cybersecurity.csv");
    Path tempOutput = new Path("output/temp");
    Path finalOutput = new Path("output/TopCountriesByAttackType");

    FileSystem fs = FileSystem.get(conf);
    fs.delete(tempOutput, true);
    fs.delete(finalOutput, true);

    // ===== JOB 1 =====
    Job job1 = Job.getInstance(conf, "FinancialLossAvg");
    job1.setJarByClass(AttackFinancialLossByCountry.class);

    FileInputFormat.addInputPath(job1, input);
    FileOutputFormat.setOutputPath(job1, tempOutput);

    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    job1.setMapperClass(FinancialLossMapper.class);
    job1.setCombinerClass(FinancialLossCombiner.class);
    job1.setReducerClass(FinancialLossReducer.class);

    job1.setMapOutputKeyClass(AttackCountryKey.class);
    job1.setMapOutputValueClass(FinancialLossValue.class);
    job1.setOutputKeyClass(AttackCountryKey.class);
    job1.setOutputValueClass(DoubleWritable.class);

    if (!job1.waitForCompletion(true)) {
      return 1;
    }

    // ===== JOB 2 =====
    Job job2 = Job.getInstance(conf, "TopCountries");
    job2.setJarByClass(AttackFinancialLossByCountry.class);

    job2.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job2, tempOutput);
    FileOutputFormat.setOutputPath(job2, finalOutput);

    job2.setMapperClass(TopCountriesMapper.class);
    job2.setReducerClass(TopCountriesReducer.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(CountryLossValue.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    return job2.waitForCompletion(true) ? 0 : 1;
  }
}