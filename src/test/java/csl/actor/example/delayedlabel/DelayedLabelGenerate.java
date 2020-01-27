package csl.actor.example.delayedlabel;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DelayedLabelGenerate {
    public static void main(String[] args) throws Exception {
        String nums = args[0];
        int numInstances = Integer.parseInt(nums.replaceAll("_", ""));

        try (PrintWriter out = out(args)) {
            List<Object> inputs = new DelayedLabelManual().generateInput(12345L, 100, 300, numInstances, 100);

            out.println(numInstances);
            for (Object i : inputs) {
                if (i instanceof DelayedLabelManual.LabelInstance) {
                    DelayedLabelManual.LabelInstance li = (DelayedLabelManual.LabelInstance) i;
                    out.println(String.format("L,%d,%d", li.getId(), li.getLabel()));
                } else if (i instanceof DelayedLabelManual.FeatureInstance) {
                    DelayedLabelManual.FeatureInstance fi = (DelayedLabelManual.FeatureInstance) i;
                    out.println(String.format("F,%d,%s", fi.getId(),
                            Arrays.stream(fi.getVector())
                                    .mapToObj(d -> String.format("%6f", d))
                                    .collect(Collectors.joining(","))));
                }
            }
        }
    }

    private static PrintWriter out(String[] args) throws Exception {
        PrintWriter out;
        if (args.length >= 2) {
            File file = new File(args[1]);
            if (file.getParentFile() != null && !file.getParentFile().exists()) {
                file.mkdirs();
            }
            out = new PrintWriter(file);
        } else {
            out = new PrintWriter(System.out);
        }
        return out;
    }
}
