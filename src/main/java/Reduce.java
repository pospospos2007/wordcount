import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;

public  class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	private Logger logger = Logger.getLogger(Reduce.class);

	
    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        logger.info("("+key+", "+sum+")");
        context.write(key, new IntWritable(sum));
    }
 }