package advanced;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import java.util.TreeMap;

public class AttackTrendAnalysis extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        BasicConfigurator.configure();
        Configuration conf = new Configuration();

        Path input = new Path("in/cybersecurity.csv");
        Path temp = new Path("output/TempAttackTrends");
        Path output = new Path("output/AttackTrendAnalysis");

        // Job 1: Contagem de ataques por tipo/país/ano
        Job job1 = Job.getInstance(conf, "Attack Count by Type/Country/Year");
        job1.setJarByClass(AttackTrendAnalysis.class);
        job1.setMapperClass(AttackCountMapper.class);
        job1.setReducerClass(AttackCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, input);
        FileSystem.get(conf).delete(temp, true);
        FileOutputFormat.setOutputPath(job1, temp);

        if (!job1.waitForCompletion(true)) {
            return 1;
        }

        // Job 2: Análise de tendências
        Job job2 = Job.getInstance(conf, "Attack Trend Analysis");
        job2.setJarByClass(AttackTrendAnalysis.class);
        job2.setMapperClass(TrendAnalysisMapper.class);
        job2.setReducerClass(TrendAnalysisReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, temp);
        FileSystem.get(conf).delete(output, true);
        FileOutputFormat.setOutputPath(job2, output);

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new AttackTrendAnalysis(), args);
        System.exit(exitCode);
    }

    // Job 1: Mapper - Emite ((País, Ano, TipoAtaque), 1)
    public static class AttackCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text compositeKey = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("Country")) return;

            String[] fields = value.toString().split(",");
            if (fields.length >= 4) {
                String country = fields[0];
                String year = fields[1];
                String attackType = fields[2];

                compositeKey.set(String.join("|", country, year, attackType));
                context.write(compositeKey, one);
            }
        }
    }

    // Job 1: Reducer - Soma as ocorrências
    public static class AttackCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Job 2: Mapper - Reorganiza dados para análise de tendências
    public static class TrendAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\\s+");
            if (parts.length == 2) {
                String[] keyParts = parts[0].split("\\|");
                if (keyParts.length == 3) {
                    String country = keyParts[0];
                    String year = keyParts[1];
                    String attackType = keyParts[2];
                    String count = parts[1];

                    outputKey.set(country + "|" + attackType);
                    outputValue.set(year + ":" + count);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    // Job 2: Reducer - Calcula tendência
    public static class TrendAnalysisReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 1. Organizar dados por ano
            TreeMap<Integer, Integer> yearCounts = new TreeMap<>();
            for (Text val : values) {
                String[] parts = val.toString().split(":");
                yearCounts.put(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
            }

            // 2. Calcular métricas
            if (yearCounts.size() >= 2) {
                int firstYear = yearCounts.firstKey();
                int lastYear = yearCounts.lastKey();
                int firstCount = yearCounts.get(firstYear);
                int lastCount = yearCounts.get(lastYear);
                double growth = ((lastCount - firstCount) / (double) firstCount) * 100;

                // 3. Gerar saída formatada
                StringBuilder trendDetails = new StringBuilder("\n");
                trendDetails.append(String.format("Period: %d-%d\n", firstYear, lastYear));
                trendDetails.append(String.format("Total Occurrences: %d\n", yearCounts.values().stream().mapToInt(i -> i).sum()));

                // Detalhes por ano
                trendDetails.append("Yearly Breakdown:\n");
                yearCounts.forEach((year, count) ->
                        trendDetails.append(String.format("  %d: %d attacks\n", year, count))
                );

                // Tendência
                String trendIcon = growth >= 0 ? "▲" : "▼";
                trendDetails.append(String.format(
                        "Trend: %s %.1f%% (%d → %d)\n",
                        trendIcon, Math.abs(growth), firstCount, lastCount
                ));

                result.set(trendDetails.toString());
                context.write(key, result);
            }
        }
    }
}