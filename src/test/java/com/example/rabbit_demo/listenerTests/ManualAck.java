package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class ManualAck {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest
    public static class Conf {
       @Bean
        Queue queue1() {
           return QueueBuilder.durable("manual.acks.1").build();
        }

        @Bean
        Queue queue2() {
            return QueueBuilder.durable("manual.acks.2").build();
        }

        @Bean
        ConnectionFactory connectionFactory() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        public RabbitAdmin admin(ConnectionFactory cf) {
            return new RabbitAdmin(cf);
        }

        @Bean
        RabbitTemplate template(ConnectionFactory cf) {
            return new RabbitTemplate(cf);
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf) {
            SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();
            containerFactory.setConnectionFactory(cf);
            containerFactory.setMismatchedQueuesFatal(false);
            containerFactory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
            containerFactory.setDefaultRequeueRejected(false);
            return containerFactory;
        }
    }

    @RabbitListener(id = "manual.acks.2", queues = "manual.acks.2", ackMode = "MANUAL") // on the container factory is enough there we can change the ackMode from Container factory
    public void manual1(String in, Channel channel,
                        @Header(AmqpHeaders.DELIVERY_TAG) long tag) throws IOException {
        log.info("received: " + in);
        if (in.equals("nnn")) {
            /**
             * difference from reject is that we can nack multiple messages at once
             */
            channel.basicNack(tag, false, false);
        } else if (in.equals("rrr")) {
            channel.basicReject(tag, false);
        } else if (in.equals("aaa")) {
            channel.basicAck(tag, false);
        }
    }

    void processMessage(Message message) {
        log.info("Processing message: {}", message);
        if (message.getMessageProperties().getDeliveryTag() % 2 == 0) {
            throw new RuntimeException("Simulated processing error");
        }
    }


    @RabbitListener(id = "manual.acks.1", queues = "manual.acks.1", ackMode = "MANUAL")
    public void handleRecomendedApproach(Message message, Channel channel) {
        try {
            // Process the message
            processMessage(message);
            // Acknowledge the message if processing succeeds
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);
        } catch (Exception e) {
            // Discard the message and log the error
            log.error("discarding message: {}", e.getMessage());
            try {
                channel.basicReject(message.getMessageProperties().getDeliveryTag(), false);
            } catch (IOException ioException) {
                log.error("Failed to reject message", ioException);
            }
        } finally {
            latch.countDown();
        }
    }

    @Autowired
    RabbitTemplate rabbitTemplate;

    private CountDownLatch latch = new CountDownLatch(2);

    @Test
    public void test() throws InterruptedException {
        rabbitTemplate.convertAndSend("manual.acks.1", "nnn");
        rabbitTemplate.convertAndSend("manual.acks.1", "eee");
        latch.await(5, TimeUnit.SECONDS);
    }


}
