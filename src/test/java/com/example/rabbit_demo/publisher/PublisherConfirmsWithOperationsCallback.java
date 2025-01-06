package com.example.rabbit_demo.publisher;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.ReturnedMessage;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@SpringBootTest
@EnableRabbit
public class PublisherConfirmsWithOperationsCallback {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public static final String ROUTE = "test.queue.PublisherConfirmsWithOperationsCallback";


    @Configuration
    public static class Config {
        @Bean
        public ConnectionFactory connectionFactory() {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
            connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            return connectionFactory;
        }

        @Bean
        public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
            RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.setMandatory(true);
            return rabbitTemplate;
        }
    }

    @Test
    public void testConfirmCallback() {

        List<Message> messages = List.of(new Message("fff".getBytes()), new Message("ggg".getBytes()));

        Boolean result = this.rabbitTemplate.invoke(t -> {
            messages.forEach(m -> t.convertAndSend(ROUTE, m));
            t.waitForConfirmsOrDie(10_000);
            return true;
        });
    }

    @Test
    public void testConfirmCorrelationDataNack() throws ExecutionException, InterruptedException, TimeoutException {
        CorrelationData correlationData = new CorrelationData();
        Object o = new Object();
        o= "foo";
        rabbitTemplate.convertAndSend("notExistingExchange","myQueue", o, correlationData);
        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        if (confirm.isAck()){
            ReturnedMessage returned = correlationData.getReturned();
            log.debug("Returned message: " + returned);
            log.debug("Returned message: " + returned.getReplyText());
            log.debug("Returned message: " + returned.getReplyCode());
            log.debug("Returned message: " + returned.getExchange());
            log.debug("Returned message: " + returned.getRoutingKey());
        } else {
            log.error(confirm.getReason());
        }
    }

    @Test
    public void testConfirmCorrelationDataAck() throws ExecutionException, InterruptedException, TimeoutException {
        CorrelationData correlationData = new CorrelationData();
        Object o = "foo";
        rabbitTemplate.convertAndSend("myQueue", o, correlationData);
        CorrelationData.Confirm confirm = correlationData.getFuture().get(10, TimeUnit.SECONDS);
        if (confirm.isAck()){
            ReturnedMessage returned = correlationData.getReturned();
            log.info("Returned message: " + returned);
            log.info("Returned message: " + returned.getReplyText());
            log.info("Returned message: " + returned.getReplyCode());
            log.info("Returned message: " + returned.getExchange());
            log.info("Returned message: " + returned.getRoutingKey());
        } else {
            log.error(confirm.getReason());
        }
    }


}
