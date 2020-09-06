import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class CommonUtils {

    public static Map<String, String> splitMap(Map<String, String> totalMap, int threadNum, int num) {
        HashMap<String, String> subMap = new HashMap<String, String>();

        int i = 0;
        for (Map.Entry<String, String> entry: totalMap.entrySet()) {
            if (i % threadNum == num) {
                subMap.put(entry.getKey(), entry.getValue());
            }
            i++;
        }

        return subMap;
    }

    public static Map<String, String> getCmdParams(String[] args) throws ParseException {
        Map<String, String> paramsMap = new HashMap<String, String>();

        CommandLineParser parser = new BasicParser();
        Options options = new Options();
        options.addOption("n", "threadsNum", true, "threads");
        options.addOption("f", "filePath", true, "filePath");
        options.addOption("i", "ip", true, "server ip");
        options.addOption("p", "port", true, "kafka port");
        options.addOption("j", "period", true, "timer period");
        options.addOption("m", "prompt", true, "prompt");
        CommandLine commandLine = parser.parse(options, args);

        if (commandLine.hasOption('n')) {
            paramsMap.put("threadsNum", commandLine.getOptionValue('n'));
        }
        if (commandLine.hasOption('f')) {
            paramsMap.put("fp", commandLine.getOptionValue('f'));
        }
        if (commandLine.hasOption('i')) {
            paramsMap.put("ip", commandLine.getOptionValue('i'));
        }
        if (commandLine.hasOption('p')) {
            paramsMap.put("port", commandLine.getOptionValue('p'));
        }
        if (commandLine.hasOption('j')) {
            paramsMap.put("period", commandLine.getOptionValue('j'));
        }
        if (commandLine.hasOption('m')) {
            paramsMap.put("prompt", commandLine.getOptionValue('m'));
        }

        return paramsMap;
    }

    public static Properties getProps(String ip, String port) {
        Properties props = new Properties();

        props.put("bootstrap.servers", ip + ":" + port);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }


    public static Map<String, String> getVinVidMap(String filePath) {

        Map<String, String> map = new HashMap<String, String>();

        try {
            BufferedReader bf = new BufferedReader(new FileReader(filePath));
            String line;
            while ((line = bf.readLine()) != null) {
                String[] split = line.split("\t");
                String vin = split[0];
                String vid = split[1];
                map.put(vin, vid);
            }
            bf.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return map;
    }

}
