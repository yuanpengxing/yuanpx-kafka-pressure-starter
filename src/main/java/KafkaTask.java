import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

class KafkaTask extends TimerTask {
    Producer<String, String> producer;
    Map<String, String> subVinVidMap;
    int threadsNum;
    SimpleDateFormat sdf;
    String prompt;

    public KafkaTask(Map<String, String> subVinVidMap, Producer<String, String> producer, int threadsNum, SimpleDateFormat sdf, String prompt) {
        this.subVinVidMap = subVinVidMap;
        this.producer = producer;
        this.threadsNum = threadsNum;
        this.sdf = sdf;
        this.prompt = prompt;
    }

    public void run() {
//        long start = new Date().getTime();
        int requests = 0;


//        Calendar calendar = Calendar.getInstance();
//        calendar.setTime(new Date());					//放入Date类型数据
//
//        calendar.get(Calendar.YEAR);					//获取年份
//        calendar.get(Calendar.MONTH);					//获取月份
//        calendar.get(Calendar.DATE);//获取日
//
//        calendar.get(Calendar.HOUR);					//时（12小时制）
//        calendar.get(Calendar.HOUR_OF_DAY);			//时（24小时制）
//        calendar.get(Calendar.MINUTE);				//分
//        calendar.get(Calendar.SECOND);

        String mtime = sdf.format(new Date());
        StringBuilder s = new StringBuilder(mtime);
        String today = s.substring(8, 10);
        String hour = s.substring(8, 10);
        String min = s.substring(8, 10);
        String sec = s.substring(8, 10);

        for (Map.Entry<String, String> entry: subVinVidMap.entrySet()) {
            StringBuffer sb = new StringBuffer();
            String vin = entry.getKey();
            sb.append("SUBMIT 0 ");
            sb.append(vin);
            sb.append(" REALTIME {VID:");
            sb.append(entry.getValue());
            sb.append("sdasdasdasd");
            sb.append(today);
            sb.append(",4408:");
            sb.append(hour);
            sb.append(",4407:");
            sb.append(min);
            sb.append(",4405:");
            sb.append(sec);
            sb.append(",9999:20190510154535}");

            producer.send(new ProducerRecord<String, String>("us_general_1", sb.toString()));
            requests++;
        }

//        long end = new Date().getTime();
//        System.out.println("Time consumed per thread: " + (end - start));

        TestMain.setTotalRequestsNum(requests);
        TestMain.setFlagNum(1);

        if (TestMain.getFlagNum() == threadsNum) {
            System.out.println(mtime + prompt + TestMain.getTotalRequestsNum());
            TestMain.resetTotalRequestsNum();
            TestMain.resetFlagNum();
        }
    }
}
