import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestMain {
    public static int totalRequestsNum = 0;
    public static int flagNum = 0;

    public static void main(String[] args) throws IOException, ParseException {
        String fp = "C:\\Users\\Administrator\\Desktop\\TEST\\vinvid.txt";
        String prompt = " request num per second is: ";
        int threadsNum = 10;
        String ip = "127.0.0.1";
        String port = "9090";
        long period = 1000;

        Map<String, String> cmdParams = CommonUtils.getCmdParams(args);
        if (cmdParams.containsKey("threadsNum")) {
            threadsNum = Integer.parseInt(cmdParams.get("threadsNum"));
        }
        if (cmdParams.containsKey("fp")) {
            fp = cmdParams.get("fp");
        }
        if (cmdParams.containsKey("ip")) {
            ip = cmdParams.get("ip");
        }
        if (cmdParams.containsKey("port")) {
            port = cmdParams.get("port");
        }
        if (cmdParams.containsKey("period")) {
            period = Integer.parseInt(cmdParams.get("period"));
        }
        if (cmdParams.containsKey("prompt")) {
            prompt = cmdParams.get("prompt");
        }
        System.out.println("threadsNum: " + threadsNum + " filePath: " + fp + " ip: "
                + ip + " port: " + port + " timerPeriod: " + period);

        Properties props = CommonUtils.getProps(ip, port);

        Map<String, String> totalVinVidMap = CommonUtils.getVinVidMap(fp);

        for (int i = 0; i < threadsNum; i++) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map<String, String> subMap = CommonUtils.splitMap(totalVinVidMap, threadsNum, i);
            Producer<String, String> producer = new KafkaProducer<String, String>(props);
            Timer timer = new Timer();
            timer.schedule(new KafkaTask(subMap, producer, threadsNum, sdf, prompt), new Date(), period);
        }
    }

    public static void setTotalRequestsNum(int num) {
        synchronized (TestMain.class) {
            totalRequestsNum += num;
        }
    }

    public static int getTotalRequestsNum() {
        synchronized (TestMain.class) {
            return totalRequestsNum;
        }
    }

    public static void resetTotalRequestsNum() {
        synchronized (TestMain.class) {
            totalRequestsNum = 0;
        }
    }

    public static void setFlagNum(int number) {
        synchronized (TestMain.class) {
            flagNum += number;
        }
    }

    public static int getFlagNum() {
        synchronized (TestMain.class) {
            return flagNum;
        }
    }

    public static void resetFlagNum() {
        synchronized (TestMain.class) {
            flagNum = 0;
        }
    }
}


