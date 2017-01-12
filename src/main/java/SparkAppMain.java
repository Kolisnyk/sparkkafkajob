
        import com.fasterxml.jackson.databind.ObjectMapper;
        import org.apache.spark.SparkConf;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import java.io.IOException;
        import java.util.*;
        import java.util.stream.Collectors;


        public class SparkAppMain {
            private static final String topic = "test";
            private static String message;

            public static void main(String[] args) throws IOException {
                SparkConf sparkConf = new SparkConf()
                        .setAppName("Calculation");
                JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
                JavaRDD<String> stringJavaRDD = sparkContext.textFile("/home/oleksii/bd/uservisits/");

                //JavaRDD<String> countries = stringJavaRDD.map(x -> x.substring(142, 144));
                JavaRDD<String> countries = stringJavaRDD.map(s -> s.split(",")).map(r -> r[5]);
                Map<String, Long> countriesCount = countries.countByValue();
                Map<String, Long> sorted = sortByValue(countriesCount);

                Set<Map.Entry<String, Long>> set = sorted.entrySet();
                List<Map.Entry<String, Long>> list = new ArrayList<>(set);
                List<Map.Entry<String, Long>> topTenCountries = new ArrayList<>();
                for (int i = 0; i < 10; i++){
                    topTenCountries.add(list.get(i));
                }
                //kreating kafka producer
                message = toJSON(topTenCountries);
                KafkaConnection kc = new KafkaConnection();
                kc.main(topic, message);
                System.out.println(message);
            }

            // sorting the Map
            private static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
                return map.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (e1, e2) -> e1,
                                LinkedHashMap::new
                        ));
            }

            // creating JSON
            private static String toJSON (List<Map.Entry<String, Long>> topTenCountries){
                String jsonString = "";
                ObjectMapper mapperObj = new ObjectMapper();
                try {
                    jsonString = mapperObj.writeValueAsString(topTenCountries);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return jsonString;
            }

        }
