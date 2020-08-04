package com.hcosta.learning.activemq;

import java.io.Closeable;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class HelloWorldProducer implements Closeable{

	/**
	 	<transportConnectors>
            <!-- DOS protection, limit concurrent connections to 1000 and frame size to 100MB -->
            <transportConnector name="openwire" uri="tcp://0.0.0.0:61616?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="amqp" uri="amqp://0.0.0.0:5672?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="stomp" uri="stomp://0.0.0.0:61613?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="mqtt" uri="mqtt://0.0.0.0:1883?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
            <transportConnector name="ws" uri="ws://0.0.0.0:61614?maximumConnections=1000&amp;wireFormat.maxFrameSize=104857600"/>
        </transportConnectors>
	 */
	public static String host = "tcp://127.0.0.1:61616";
	public static String queue = "hello.world.queue";
	public static String user = "admin";
	public static String password = "admin";


	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;

	public HelloWorldProducer() throws JMSException {
		// Create a ConnectionFactory
		connectionFactory = new ActiveMQConnectionFactory(user, password, host);
		// Create a Connection
		connection = connectionFactory.createConnection();
		connection.start();

		// Create a Session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}

	public void sendMessage() {
		try {
			// Create the destination (Topic or Queue)
			Destination destination = session.createQueue(queue);

			// Create a MessageProducer from the Session to the Topic or Queue
			MessageProducer producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.PERSISTENT);

			// Create a messages
			String text = "Hello world! From: " + Thread.currentThread().getName() + " : " + this.hashCode();
			TextMessage message = session.createTextMessage(text);

			// Tell the producer to send the message
			System.out.println("Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName());
			producer.send(message);


		}
		catch (Exception e) {
			System.out.println("Caught: " + e);
			e.printStackTrace();
		}
	}

	@Override
	public void close() throws IOException {
		// Clean up
		try {
			session.close();
			connection.close();
		} catch (JMSException e) {
			throw new IOException(e);
		}
	}



	public static void main(String[] args) throws IOException, JMSException {
		try (HelloWorldProducer helloWorldProducer = new HelloWorldProducer()) {
			for(int i=0;i<10;i++)
				helloWorldProducer.sendMessage();
		}
	}





}
