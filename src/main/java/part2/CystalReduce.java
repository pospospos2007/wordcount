package part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public  class CystalReduce extends Reducer<Pair, IntWritable, Text, IntWritable> {

	private Logger logger = Logger.getLogger(CystalReduce.class);

	
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        logger.info("("+key.toString()+", "+sum+")");
        context.write(key, new IntWritable(sum));
    }
 }