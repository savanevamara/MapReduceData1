package summaries;


/*

Copyright : Savane Vamara

11/01/2018

*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Main extends Configured implements Tool {


    @Override
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance(getConf());
        job.setJobName("average");
        job.setJarByClass(Main.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path inputPathFile = new Path("/Users/admin/IdeaProjects/MapReduceData1/data/input/census.txt");
        Path outputPathFile = new Path("/Users/admin/IdeaProjects/MapReduceData1/data/output");

        FileInputFormat.addInputPath(job,inputPathFile);
        FileOutputFormat.setOutputPath(job,outputPathFile);


        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Main(),args);

        System.exit(exitCode);
    }

}
