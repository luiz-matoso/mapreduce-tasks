package advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

public class AttackPatternAnalysis extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        // Paths para os arquivos
        Path input = new Path("in/cybersecurity.csv");
        Path temp = new Path("output/TempAttackPatterns");
        Path output = new Path("output/AttackPatternAnalysis");

        // Primeiro Job - Contagem de padrões
        Job job1 = Job.getInstance(conf, "Count Attack Patterns");
        job1.setJarByClass(AttackPatternAnalysis.class);
        job1.setMapperClass(PatternMapper.class);
        job1.setReducerClass(PatternReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(AttackStats.class);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(temp, true);
        FileOutputFormat.setOutputPath(job1, temp);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        // Segundo Job - Padrões mais comuns por ano
        Job job2 = Job.getInstance(conf, "Find Top Patterns Per Year");
        job2.setJarByClass(AttackPatternAnalysis.class);
        job2.setMapperClass(TopPatternMapper.class);
        job2.setReducerClass(TopPatternReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, temp);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job2, output);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AttackPatternAnalysis(), args);
        System.exit(exitCode);
    }

    // Classe Writable personalizada
    public static class AttackStats implements Writable {
        private int count;
        private double totalResolutionTime;

        public AttackStats() {
            this(0, 0);
        }

        public AttackStats(int count, double totalResolutionTime) {
            this.count = count;
            this.totalResolutionTime = totalResolutionTime;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(count);
            out.writeDouble(totalResolutionTime);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            count = in.readInt();
            totalResolutionTime = in.readDouble();
        }

        public int getCount() {
            return count;
        }

        public double getTotalResolutionTime() {
            return totalResolutionTime;
        }

        public void add(AttackStats other) {
            this.count += other.getCount();
            this.totalResolutionTime += other.getTotalResolutionTime();
        }

        public double getAverageResolutionTime() {
            return count == 0 ? 0 : totalResolutionTime / count;
        }

        @Override
        public String toString() {
            return count + "," + String.format("%.2f", getAverageResolutionTime());
        }
    }

    // Mapper e Reducer do primeiro job
    public static class PatternMapper extends Mapper<LongWritable, Text, Text, AttackStats> {
        private Text yearAttackVulnerability = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("Country")) {
                return; // Pular cabeçalho
            }

            String[] fields = value.toString().split(",");
            if (fields.length >= 10) {
                try {
                    String year = fields[1];
                    String attackType = fields[2];
                    String vulnerabilityType = fields[7];
                    double resolutionTime = Double.parseDouble(fields[9]);

                    yearAttackVulnerability.set(year + ":" + attackType + ":" + vulnerabilityType);
                    context.write(yearAttackVulnerability, new AttackStats(1, resolutionTime));
                } catch (Exception e) {
                    // Ignorar registros malformados
                }
            }
        }
    }

    public static class PatternReducer extends Reducer<Text, AttackStats, Text, AttackStats> {
        private AttackStats result = new AttackStats();

        public void reduce(Text key, Iterable<AttackStats> values, Context context)
                throws IOException, InterruptedException {
            int totalCount = 0;
            double totalTime = 0;

            for (AttackStats val : values) {
                totalCount += val.getCount();
                totalTime += val.getTotalResolutionTime();
            }

            result = new AttackStats(totalCount, totalTime);
            context.write(key, result);
        }
    }

    // Mapper e Reducer do segundo job
    public static class TopPatternMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text year = new Text();
        private Text patternStats = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                String[] keyParts = parts[0].split(":");
                if (keyParts.length == 3) {
                    String yearStr = keyParts[0];
                    String attackType = keyParts[1];
                    String vulnerabilityType = keyParts[2];
                    String[] stats = parts[1].split(",");

                    if (stats.length == 2) {
                        year.set(yearStr);
                        patternStats.set(attackType + ":" + vulnerabilityType + ":" + stats[0] + ":" + stats[1]);
                        context.write(year, patternStats);
                    }
                }
            }
        }
    }

    public static class TopPatternReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // Usar TreeMap para ordenar por contagem (decrescente)
            TreeMap<Integer, String> patterns = new TreeMap<>(Collections.reverseOrder());

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length == 4) {
                    int count = Integer.parseInt(parts[2]);
                    double avgTime = Double.parseDouble(parts[3]);
                    String pattern = parts[0] + " with " + parts[1] +
                            " (Avg Resolution: " + String.format("%.2f", avgTime) + " hours)";
                    patterns.put(count, pattern);
                }
            }

            // Pegar os top 3
            StringBuilder sb = new StringBuilder();
            int rank = 0;
            for (Map.Entry<Integer, String> entry : patterns.entrySet()) {
                if (rank >= 3) break;
                sb.append("\n").append(rank + 1).append(". ").append(entry.getValue());
                rank++;
            }

            result.set(sb.toString());
            context.write(key, result);
        }
    }
}