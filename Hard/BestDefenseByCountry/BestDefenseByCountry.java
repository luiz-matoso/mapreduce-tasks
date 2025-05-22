package advanced.bestdefensebycountry;

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

public class BestDefenseByCountry extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();
        int exitCode = ToolRunner.run(new BestDefenseByCountry(), args);
        System.exit(exitCode);
    }

    // ========== JOB 1: Calculate average resolution time by (Country, Defense) ==========

    public static class DefenseStatsMapper extends Mapper<LongWritable, Text, CountryDefenseKey, DefenseStatsValue> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("Country")) {
                return; // Skip header
            }

            String[] columns = value.toString().split(",");
            String country = columns[0];
            String defense = columns[8];
            int resolutionTime = Integer.parseInt(columns[9]);

            CountryDefenseKey outKey = new CountryDefenseKey(country, defense);
            DefenseStatsValue outValue = new DefenseStatsValue(resolutionTime, 1);

            context.write(outKey, outValue);
        }
    }

    public static class DefenseStatsCombiner extends Reducer<CountryDefenseKey, DefenseStatsValue, CountryDefenseKey, DefenseStatsValue> {
        public void reduce(CountryDefenseKey key, Iterable<DefenseStatsValue> values, Context context)
                throws IOException, InterruptedException {
            int totalTime = 0;
            int count = 0;

            for (DefenseStatsValue v : values) {
                totalTime += v.getResolutionTime();
                count += v.getCount();
            }

            context.write(key, new DefenseStatsValue(totalTime, count));
        }
    }

    public static class DefenseStatsReducer extends Reducer<CountryDefenseKey, DefenseStatsValue, CountryDefenseKey, DoubleWritable> {
        public void reduce(CountryDefenseKey key, Iterable<DefenseStatsValue> values, Context context)
                throws IOException, InterruptedException {
            int totalTime = 0;
            int count = 0;

            for (DefenseStatsValue v : values) {
                totalTime += v.getResolutionTime();
                count += v.getCount();
            }

            double avgTime = (double) totalTime / count;
            context.write(key, new DoubleWritable(avgTime));
        }
    }

    // ========== JOB 2: Find best defense (lowest avg time) for each country ==========

    public static class BestDefenseMapper extends Mapper<CountryDefenseKey, DoubleWritable, Text, DefenseAvgValue> {
        public void map(CountryDefenseKey key, DoubleWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(new Text(key.getCountry()),
                    new DefenseAvgValue(key.getDefense(), value.get()));
        }
    }

    public static class BestDefenseReducer extends Reducer<Text, DefenseAvgValue, Text, Text> {
        public void reduce(Text key, Iterable<DefenseAvgValue> values, Context context)
                throws IOException, InterruptedException {
            String bestDefense = null;
            double minTime = Double.MAX_VALUE;

            for (DefenseAvgValue v : values) {
                if (v.getAvgTime() < minTime) {
                    minTime = v.getAvgTime();
                    bestDefense = v.getDefense();
                }
            }

            if (bestDefense != null) {
                String result = String.format("Best Defense: %s (Avg Time: %.2f hours)", bestDefense, minTime);
                context.write(key, new Text(result));
            }
        }
    }

    // ========== Custom Writable Classes ==========

    public static class CountryDefenseKey implements WritableComparable<CountryDefenseKey> {
        private Text country;
        private Text defense;

        public CountryDefenseKey() {
            this.country = new Text();
            this.defense = new Text();
        }

        public CountryDefenseKey(String country, String defense) {
            this.country = new Text(country);
            this.defense = new Text(defense);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            country.write(out);
            defense.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            country.readFields(in);
            defense.readFields(in);
        }

        @Override
        public int compareTo(CountryDefenseKey o) {
            int cmp = country.compareTo(o.country);
            if (cmp != 0) return cmp;
            return defense.compareTo(o.defense);
        }

        public String getCountry() { return country.toString(); }
        public String getDefense() { return defense.toString(); }

        @Override
        public String toString() {
            return country + "," + defense;
        }
    }

    public static class DefenseStatsValue implements Writable {
        private IntWritable resolutionTime;
        private IntWritable count;

        public DefenseStatsValue() {
            this.resolutionTime = new IntWritable();
            this.count = new IntWritable();
        }

        public DefenseStatsValue(int resolutionTime, int count) {
            this.resolutionTime = new IntWritable(resolutionTime);
            this.count = new IntWritable(count);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            resolutionTime.write(out);
            count.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            resolutionTime.readFields(in);
            count.readFields(in);
        }

        public int getResolutionTime() { return resolutionTime.get(); }
        public int getCount() { return count.get(); }
    }

    public static class DefenseAvgValue implements Writable {
        private Text defense;
        private DoubleWritable avgTime;

        public DefenseAvgValue() {
            this.defense = new Text();
            this.avgTime = new DoubleWritable();
        }

        public DefenseAvgValue(String defense, double avgTime) {
            this.defense = new Text(defense);
            this.avgTime = new DoubleWritable(avgTime);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            defense.write(out);
            avgTime.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            defense.readFields(in);
            avgTime.readFields(in);
        }

        public String getDefense() { return defense.toString(); }
        public double getAvgTime() { return avgTime.get(); }
    }

    // ========== Driver Code ==========

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path input = new Path("in/cybersecurity.csv");
        Path tempOutput = new Path("output/temp");
        Path finalOutput = new Path("output/BestDefenseByCountry");

        // Clean up previous runs
        FileSystem fs = FileSystem.get(conf);
        fs.delete(tempOutput, true);
        fs.delete(finalOutput, true);

        // ===== JOB 1 =====
        Job job1 = Job.getInstance(conf, "DefenseStats");
        job1.setJarByClass(BestDefenseByCountry.class);

        FileInputFormat.addInputPath(job1, input);
        FileOutputFormat.setOutputPath(job1, tempOutput);

        // Configure Job1 to output SequenceFiles
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setMapperClass(DefenseStatsMapper.class);
        job1.setCombinerClass(DefenseStatsCombiner.class);
        job1.setReducerClass(DefenseStatsReducer.class);

        job1.setMapOutputKeyClass(CountryDefenseKey.class);
        job1.setMapOutputValueClass(DefenseStatsValue.class);
        job1.setOutputKeyClass(CountryDefenseKey.class);
        job1.setOutputValueClass(DoubleWritable.class);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        // ===== JOB 2 =====
        Job job2 = Job.getInstance(conf, "BestDefense");
        job2.setJarByClass(BestDefenseByCountry.class);

        // Configure Job2 to read SequenceFiles
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job2, tempOutput);
        FileOutputFormat.setOutputPath(job2, finalOutput);

        job2.setMapperClass(BestDefenseMapper.class);
        job2.setReducerClass(BestDefenseReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DefenseAvgValue.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

}