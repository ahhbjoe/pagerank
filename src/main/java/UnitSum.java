import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * The UnitSum class implements the second step of PageRank, viz. summation of the unit products with the same "to"
 * webpage.
 */
public class UnitSum {

    /**
     * The PassMapper class reads the output of unit multiplication and maps each line to a key-value pair. The key is
     * the "to" webpage. The value is the unit product.
     */
    public static class PassMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueList = value.toString().trim().split("\t");
            context.write(new Text(valueList[0]), new DoubleWritable(Double.parseDouble(valueList[1])));
        }
    }

    /**
     * The SumReducer class calculates the summation of the unit products with the same "to" webpage and writes the
     * result as a key-value pair. The key is the "to" webpage. The value is the summation, i.e. the new PageRank value.
     */
    public static class SumReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable value : values) {
                sum += Double.parseDouble(value.toString());
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();

        job.setJarByClass(UnitSum.class);

        job.setMapperClass(PassMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
