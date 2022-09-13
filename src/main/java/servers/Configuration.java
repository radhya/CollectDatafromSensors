package servers;



import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 
 * @author Radhya Sahal
 * @email radhya.sahal@gmail.com
 *  2018
 *
 */
public class Configuration {
	
	final Logger logger = LoggerFactory.getLogger(Configuration.class);

	/**
	 * The only instance of the class (Singleton pattern)
	 */
	private static final Configuration INSTANCE = new Configuration();

	/**
	 * @return the only instance of the class (Singleton pattern)
	 */
	public static Configuration getInstance() {
		return Configuration.INSTANCE;
	}

	/**
	 * Reference to the Properties object
	 */
	private Properties prop = null;

	/**
	 * Private Constructor (Singleton pattern)
	 */
	private Configuration() {
		initialize();
	}

	/**
	 * 
	 * @return
	 */
	public String getServerPort() {
		if (prop == null) {
			logger.info("Default Http Server Port: 8080");
			return "8080";
		}
		return prop.getProperty("server_port");
	}
	
	
	/**
	 * 
	 * @return
	 */
	public String getKafkaBrokers() {
		if (prop == null) {
			logger.info("Default Kafka broker: localhost:8017");
			return "localhost:8017";
		}
		return prop.getProperty("kafka_brokers");
	}
	
	
	/**
	 * 
	 * @return
	 */
	public String getKafkaTopic() {
		if (prop == null) {
			logger.info("Default Kafka topic: android-kafka");
			return "android-kafka";
		}
		return prop.getProperty("kafka_topic");
	}
	
	
	

	/**
	 * Initializes the resourceFolder and the prop objects
	 */
	private void initialize() {
		try {
			prop = new Properties();
			prop.load(new FileInputStream(System.getProperty("user.dir")+ File.separator
					+ "config.properties"));
		} catch (final Exception e) {
			logger.error("Could not open http-kafka-config.properties file.");
			logger.error(e.toString());
			logger.info("Using default setting");
			prop = null;
		}

	}
	
}
