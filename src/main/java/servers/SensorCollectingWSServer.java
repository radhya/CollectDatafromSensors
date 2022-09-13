package servers;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;


public class SensorCollectingWSServer extends WebSocketServer{
	
	public static Logger Log = Logger.getLogger(SensorCollectingWSServer.class.getPackage().getName());
	
	String kafkaBrokers;
	
	String kafkaTopic;
	
	Producer producer;
	
	
	public SensorCollectingWSServer( int port ) throws UnknownHostException {
		super( new InetSocketAddress( port ) );
		this.kafkaBrokers = Configuration.getInstance().getKafkaBrokers();
		this.kafkaTopic = Configuration.getInstance().getKafkaTopic();
	}

	public SensorCollectingWSServer( InetSocketAddress address ) {
		super( address );
	}


	@Override
	public void onClose(WebSocket arg0, int arg1, String arg2, boolean arg3) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onError(WebSocket arg0, Exception arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onMessage(WebSocket arg0, String arg1) {
		Log.info("Receive message " + arg1);
		this.sendToKafka(arg1);
		
	}

	@Override
	public void onOpen(WebSocket arg0, ClientHandshake arg1) {
		Log.info("Get connection request from " + arg0.getRemoteSocketAddress().getAddress().getHostAddress());
		this.producer = this.startKafkaProducer(); 
		
	}

	@Override
	public void onStart() {
		// TODO Auto-generated method stub
		
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
	
	/**
	 * Send sensor observation to Kafka with kafka topic in configuration file
	 * @param str
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public void sendToKafka(String str){
		
		Log.info("Sending messgae to kafka: " + str);
        
        
        final ProducerRecord<String, String> record =
             new ProducerRecord<String, String>(this.kafkaTopic,System.currentTimeMillis()+"",str);
                
        producer.send(record);
        producer.flush();
       
        Log.info("Sent.." + str);
      
	         
	}
	
	public static void main( String[] args ) {
		
		SensorCollectingWSServer s;
		
		try {
			s = new SensorCollectingWSServer( Integer.parseInt(Configuration.getInstance().getServerPort()) );
			//s.start();
			System.out.println( "Server started on port: " + s.getPort() );

			while ( true ) {
				String str="gg";
				s.sendToKafka(str);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
