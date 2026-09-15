import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private int categoryIndex = -1;
    private int amountIndex = -1;

    private final Text outKey = new Text();
    private final DoubleWritable outValue = new DoubleWritable();

    private boolean headerProcessed = false;

    @Override
    public void map(LongWritable key,
                    Text value,
                    Context context)
                    throws IOException, InterruptedException {

        String line = value.toString();

        if (line.trim().isEmpty()) {
            return;
        }

        // First line = CSV header
        if (!headerProcessed) {

            String[] header = parseCSV(line);

            for (int i = 0; i < header.length; i++) {

                String column = header[i]
                        .replace("\uFEFF", "")
                        .trim();

                if (column.equalsIgnoreCase("Category")) {
                    categoryIndex = i;
                }

                if (column.equalsIgnoreCase("Amount")) {
                    amountIndex = i;
                }
            }

            headerProcessed = true;
            return;
        }

        // Ignore if required columns are not found
        if (categoryIndex == -1 || amountIndex == -1) {
            return;
        }

        String[] data = parseCSV(line);

        if (data.length <= categoryIndex ||
            data.length <= amountIndex) {
            return;
        }

        String category = data[categoryIndex].trim();
        String amountText = data[amountIndex].trim();

        if (category.isEmpty() || amountText.isEmpty()) {
            return;
        }

        try {

            double amount = Double.parseDouble(
                    amountText.replace(",", "")
            );

            outKey.set(category);
            outValue.set(amount);

            context.write(outKey, outValue);

        } catch (NumberFormatException e) {
            // Invalid amount is ignored
        }
    }

    // Simple CSV parser supporting quoted commas
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

                fields.add(current.toString());
                current.setLength(0);

            } else {

                current.append(c);
            }
        }

        fields.add(current.toString());

        return fields.toArray(new String[0]);
    }
}