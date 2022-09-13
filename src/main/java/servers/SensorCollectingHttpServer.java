package servers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 
 * @author thulepham
 * @email thule.pham@insight-centre.org
 * @ Insight, NUIG
 * 28 Sep 2017
 *
 */
public class SensorCollectingHttpServer {
	
	
	final Logger logger = LoggerFactory.getLogger(SensorCollectingHttpServer.class);
	
	
	String httpServerPort;
	
	String kafkaBrokers;
	
	String kafkaTopic;
	
	Producer producer;
	
	/**
	 * 
	 */
	public SensorCollectingHttpServer() {
		
		super();
		this.httpServerPort = Configuration.getInstance().getServerPort();
		this.kafkaBrokers = Configuration.getInstance().getKafkaBrokers();
		this.kafkaTopic = Configuration.getInstance().getKafkaTopic();
		logger.info(kafkaBrokers);
//		this.startKafkaProducer();
		
	}
	
	
	/**
	 * 
	 */
	public void startHttpServer(){
		ServerSocket s;
		logger.info("HttpServer starting up on port {}",this.httpServerPort);
//		System.out.println("HttpServer starting up on port {}" + this.httpServerPort);
	    try {
	      // create the main server socket
	      s = new ServerSocket(Integer.parseInt(this.httpServerPort));
	    } catch (Exception e) {
	      logger.error("Error: " + e);
	      return;
	    }
	    
	    //waiting for connection from clients
	    for (;;) {
	    	try {
		        // wait for a connection
		        Socket remote = s.accept();
		        
		        // remote is now the connected socket
		        BufferedReader in = new BufferedReader(new InputStreamReader(
		            remote.getInputStream()));
		        
		        
		        logger.info("WebServer received message at... " + System.currentTimeMillis());
		        String str = null;
		        while ((str=in.readLine())!=null){
		        	if (str.contains("sensor")){
		        		logger.info(str);
		        		break;
		        	}
		        }
		        remote.close();
		        
		        //sending received message to kafka
//		        this.sendToKafka(str);
		        
		        this.sendToKafka_multipleTopics(str);
		     
		        
		      } catch (Exception e) {
		        logger.error("Error: " + e);
		      }
		}
	}

	/**
	 * Send sensor observation to Kafka with kafka topic in configuration file
	 * @param str
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void sendToKafka(String str) throws InterruptedException, ExecutionException{
		
		logger.info("Sending messgae to kafka: " + str);
        
        this.producer = this.startKafkaProducer(); 
        
        final ProducerRecord<String, String> record =
             new ProducerRecord<String, String>(this.kafkaTopic,System.currentTimeMillis()+"",str);
        
//        RecordMetadata metadata = (RecordMetadata) producer.send(record).get();
//
//        System.out.printf("sent record(key=%s value=%s) " +
//                        "meta(partition=%d, offset=%d) time=%d\n",
//                record.key(), record.value(), metadata.partition(),
//                metadata.offset(), System.currentTimeMillis());
//        
        producer.send(record);
        producer.flush();
       
        logger.info("Sent.." + str);
        this.stopKafkaProducer();
	         
	}
	
	
	/**
	 * Send sensor observation to Kafka with kafka topic is sensor type
	 * @param str
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	private void sendToKafka_multipleTopics(String str) throws InterruptedException, ExecutionException{
		
		logger.info("Sending messgae to kafka: " + str);
        
        this.producer = this.startKafkaProducer(); 
        
        // get sensor type = kafka topic
        JSONParser parser = new JSONParser();
        JSONObject json;
		try {
			json = (JSONObject) parser.parse(str);
			String topic = (String)json.get("sensorType");
		    System.out.println(topic);
		        
		        
		    final ProducerRecord<String, String> record =
		             new ProducerRecord<String, String>(topic,System.currentTimeMillis()+"",str);
		      
		    producer.send(record);
		    producer.flush();
		       
		    logger.info("Sent.." + str);
		    this.stopKafkaProducer();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
       
	         
	}
	
	private Producer startKafkaProducer(){
		
		//Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,this.kafkaBrokers);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        Producer producer = new KafkaProducer(configProperties);
        return producer;
        
	}
	
	private void stopKafkaProducer(){
		this.producer.close();
	}
	
	public static void main(String[] args) {
		SensorCollectingHttpServer server = new SensorCollectingHttpServer();
		server.startHttpServer();

	}

}
