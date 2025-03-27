package intermediate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

class AttackDataWritable implements Writable {
    private IntWritable attackCount;
    private FloatWritable financialLoss;
    private IntWritable affectedUsers;

    public AttackDataWritable() {
        this.attackCount = new IntWritable();
        this.financialLoss = new FloatWritable();
        this.affectedUsers = new IntWritable();
    }

    public AttackDataWritable(int attackCount, float financialLoss, int affectedUsers) {
        this.attackCount = new IntWritable(attackCount);
        this.financialLoss = new FloatWritable(financialLoss);
        this.affectedUsers = new IntWritable(affectedUsers);
    }

    public IntWritable getAttackCount() {
        return attackCount;
    }

    public FloatWritable getFinancialLoss() {
        return financialLoss;
    }

    public IntWritable getAffectedUsers() {
        return affectedUsers;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        attackCount.write(out);
        financialLoss.write(out);
        affectedUsers.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        attackCount.readFields(in);
        financialLoss.readFields(in);
        affectedUsers.readFields(in);
    }

    @Override
    public String toString() {
        return attackCount + "\t" + financialLoss + "\t" + affectedUsers;
    }
}

public class AttackAnalysis {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path("in/cybersecurity.csv");
        Path output = new Path("output/attack_analysis");

        Job j = Job.getInstance(c, "attack_analysis");

        j.setJarByClass(AttackAnalysis.class);
        j.setMapperClass(AttackMapper.class);
        j.setCombinerClass(AttackCombiner.class);
        j.setReducerClass(AttackReducer.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AttackDataWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(AttackDataWritable.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class AttackMapper extends Mapper<Object, Text, Text, AttackDataWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String linha = value.toString();
            String[] colunas = linha.split(",");

            if (colunas.length >= 6) {
                String country = colunas[0].trim();
                float financialLoss = 0;
                int affectedUsers = 0;

                try {
                    financialLoss = Float.parseFloat(colunas[4].trim());
                } catch (NumberFormatException e) {
                    financialLoss = 0;
                }

                try {
                    affectedUsers = Integer.parseInt(colunas[5].trim());
                } catch (NumberFormatException e) {
                    affectedUsers = 0;
                }

                context.write(new Text(country), new AttackDataWritable(1, financialLoss, affectedUsers));
            }
        }
    }

    public static class AttackCombiner extends Reducer<Text, AttackDataWritable, Text, AttackDataWritable> {
        public void reduce(Text key, Iterable<AttackDataWritable> values, Context context) throws IOException, InterruptedException {
            int totalAttacks = 0;
            float totalLoss = 0;
            int totalAffectedUsers = 0;

            for (AttackDataWritable val : values) {
                totalAttacks += val.getAttackCount().get();
                totalLoss += val.getFinancialLoss().get();
                totalAffectedUsers += val.getAffectedUsers().get();
            }

            context.write(key, new AttackDataWritable(totalAttacks, totalLoss, totalAffectedUsers));
        }
    }

    public static class AttackReducer extends Reducer<Text, AttackDataWritable, Text, AttackDataWritable> {
        public void reduce(Text key, Iterable<AttackDataWritable> values, Context context) throws IOException, InterruptedException {
            int totalAttacks = 0;
            float totalLoss = 0;
            int totalAffectedUsers = 0;

            for (AttackDataWritable val : values) {
                totalAttacks += val.getAttackCount().get();
                totalLoss += val.getFinancialLoss().get();
                totalAffectedUsers += val.getAffectedUsers().get();
            }

            context.write(key, new AttackDataWritable(totalAttacks, totalLoss, totalAffectedUsers));
        }
    }
}