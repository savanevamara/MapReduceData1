package summaries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


/*

Copyright : Savane Vamara

11/01/2018

*/

public class Reduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {


    @Override
    public void reduce(final Text key,
                       final Iterable<DoubleWritable> values,
                       final Context context) throws IOException, InterruptedException {

        Double sum = 0.0;
        Integer count = 0;

        for (DoubleWritable value : values) {
            sum += value.get();
            count++;
        }

        // Find the ratio for every key and write it out as the final output
        Double ratio = sum / count;
        context.write(key,new DoubleWritable(ratio));
    }

}
