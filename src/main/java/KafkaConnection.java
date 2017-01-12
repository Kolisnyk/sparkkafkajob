
    import org.apache.kafka.clients.producer.KafkaProducer;
    import org.apache.kafka.clients.producer.ProducerRecord;

    import java.util.Properties;

    public class KafkaConnection {

        public static Properties properties = new Properties();
        public static KafkaProducer<String, String> producer;

        public KafkaConnection() {
        }

        public static void main(String[] args){
        }

        public static void streamKafka(String topic, String message) {

            properties.put("metadata.broker.list", "localhost:9092");
            properties.put("bootstrap.servers", "localhost:9092");
            properties.put("acks", "all");
            properties.put("retries", 0);
            properties.put("batch.size", 16384);
            properties.put("linger.ms", 1);
            properties.put("buffer.memory", 33554432);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer<String, String> producer;
            producer = new KafkaProducer<>(properties);

            new KafkaConnection();
            String topics = "";
            String messages = "";
            if (topic != null && messages != null){
                topics = topic;
                messages = message;
            }
            ProducerRecord<String, String> data = new ProducerRecord<>(topics, messages);
            producer.send(data);
            producer.close();
        }
    }

