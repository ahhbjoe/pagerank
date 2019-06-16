import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The UnitMultiplication class implements the first step of PageRank, viz. unit multiplication of the transition matrix
 * and the old PageRank matrix.
 */
public class UnitMultiplication {

    /**
     * The TransitionMapper class reads transition.txt and maps each line to a key-value pair. The key is the "from"
     * webpage. The value is the "to" webpage and the corresponding transition weight joined by "=".
     */
    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fromTos = value.toString().trim().split("\t");

            if (fromTos.length <= 1 || fromTos[1].trim().equals("")) {
                return;
            }

            String from = fromTos[0];
            String[] tos = fromTos[1].trim().split(",");

            for (String to : tos) {
                context.write(new Text(from), new Text(to + "=" + 1.0 / tos.length));
            }
        }
    }

    /**
     * The PRMapper class reads pr.txt and maps each line to a key-value pair. The key is the webpage. The value is its
     * corresponding old PageRank value.
     */
    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] pr = value.toString().trim().split("\t");
            context.write(new Text(pr[0]), new Text(pr[1]));
        }
    }

    /**
     * The MultiplicationReducer class calculates the product of each transition weight and its corresponding old
     * PageRank value and writes the result as a key-value pair. The key is the "to" webpage. The value is the product.
     */
    public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> tos = new ArrayList<String>();
            List<Double> weights = new ArrayList<Double>();
            double pr = 0;

            for (Text value : values) {
                String valueString = value.toString();
                if (valueString.indexOf('=') >= 0) {
                    String[] toWeight = valueString.trim().split("=");
                    tos.add(toWeight[0]);
                    weights.add(Double.parseDouble(toWeight[1]));
                } else {
                    pr = Double.parseDouble(valueString);
                }
            }

            for (int i = 0; i < tos.size(); i++) {
                double res = weights.get(i) * pr;
                context.write(new Text(tos.get(i)), new DoubleWritable(res));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();

        job.setJarByClass(UnitMultiplication.class);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);
    }
}
