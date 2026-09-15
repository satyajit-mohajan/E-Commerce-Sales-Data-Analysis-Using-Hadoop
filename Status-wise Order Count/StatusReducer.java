import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StatusReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context context)
                       throws IOException, InterruptedException {

        int count = 0;

        for (IntWritable value : values) {
            count += value.get();
        }

        result.set(count);

        context.write(key, result);
    }
}