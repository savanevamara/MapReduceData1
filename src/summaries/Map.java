package summaries;

/*

Copyright : Savane Vamara

11/01/2018

*/

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class Map extends Mapper<LongWritable,Text,Text,DoubleWritable> {


    @Override
    public void map(LongWritable key,Text value,Context context)
     throws IOException , InterruptedException{

        String line = value.toString();
        // The source file is CSV split by comma
        String[] data = line.split(",");

        try {

            // Marital Status 6 Column
            String maritalStatus = data[5];
            // Number of hours the person work in
            Double hrs = Double.parseDouble(data[12]);

            // The output of the map phase
            // <marital status , hours worked per week>
            context.write(new Text(maritalStatus),new DoubleWritable(hrs));
        } catch (Exception e) {

        }
    }
}
