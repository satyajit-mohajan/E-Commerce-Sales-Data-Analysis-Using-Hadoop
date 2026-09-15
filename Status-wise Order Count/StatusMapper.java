import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StatusMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private int statusIndex = -1;
    private boolean headerProcessed = false;

    private final IntWritable one = new IntWritable(1);
    private final Text outKey = new Text();

    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context)
                    throws IOException, InterruptedException {

        String line = value.toString();

        if (line.trim().isEmpty()) {
            return;
        }

        // First line is the header
        if (!headerProcessed) {

            String[] header = parseCSV(line);

            for (int i = 0; i < header.length; i++) {

                String column = header[i]
                        .replace("\uFEFF", "")
                        .trim();

                if (column.equalsIgnoreCase("Status")) {
                    statusIndex = i;
                }
            }

            headerProcessed = true;
            return;
        }

        if (statusIndex == -1) {
            return;
        }

        String[] data = parseCSV(line);

        if (data.length <= statusIndex) {
            return;
        }

        String status = data[statusIndex].trim();

        if (status.isEmpty()) {
            return;
        }

        outKey.set(status);

        context.write(outKey, one);
    }

    private String[] parseCSV(String line) {

        List<String> fields = new ArrayList<>();

        StringBuilder current = new StringBuilder();

        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char c = line.charAt(i);

            if (c == '"') {

                if (inQuotes &&
                    i + 1 < line.length() &&
                    line.charAt(i + 1) == '"') {

                    current.append('"');
                    i++;

                } else {

                    inQuotes = !inQuotes;
                }

            } else if (c == ',' && !inQuotes) {

                fields.add(current.toString().trim());
                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        fields.add(current.toString().trim());

        return fields.toArray(new String[0]);
    }
}