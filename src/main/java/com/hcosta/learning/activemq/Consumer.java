package com.hcosta.learning.activemq;

import java.io.Closeable;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public class Consumer implements Closeable{
	
	private ActiveMQConnectionFactory connectionFactory;
	private Connection connection;
	private Session session;

	public Consumer() throws JMSException {
		// Create a ConnectionFactory
		connectionFactory = new ActiveMQConnectionFactory(HelloWorldProducer.user, HelloWorldProducer.password, HelloWorldProducer.host);
		// Create a Connection
		connection = connectionFactory.createConnection();
		connection.start();

		// Create a Session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	}
	
	public void listener() throws JMSException {
		// Create the destination (Topic or Queue)
        Destination destination = session.createQueue(HelloWorldProducer.queue);

        // Create a MessageConsumer from the Session to the Topic or Queue
        MessageConsumer consumer = session.createConsumer(destination);

        // Wait for a message
        Message message = consumer.receive(1000);

        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();
            System.out.println("Received: " + text);
        } else {
//            System.out.println("Received: " + message);
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
		while(true) {
			try (Consumer consumer = new Consumer()) {
				consumer.listener();	
			}
		}

	}


}
