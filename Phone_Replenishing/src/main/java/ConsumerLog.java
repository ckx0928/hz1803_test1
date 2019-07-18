import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerConnector;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerLog {
    /*public static void main(String[] args) {
        //这个是用来配置kafka的参数
        Properties prop = new Properties();
        //这里不是配置broker.id了，这个是配置bootstrap.servers
        prop.put("bootstrap.servers", "All01:9092,All02:9092,All03:9092");
        //下面是分别配置 key和value的序列化
        prop.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        ConsumerConnector consumerConnector = (ConsumerConnector) kafka.consumer.Consumer.
                createJavaConsumerConnector(new ConsumerConfig(prop));
        Map<String,Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("hz1803b",new Integer(1));
        Map<String, List<KafkaStream<byte[],byte[]>>> comsumerMap =
                consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[],byte[]> stream = comsumerMap.get("hz1803b").get(0);
        ConsumerIterator<byte[],byte[]> it = stream.iterator();
        while(it.hasNext()) {
            System.out.println(new String(it.next().message()));
        }

    }*/
}
