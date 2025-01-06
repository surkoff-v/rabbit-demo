package com.example.rabbit_demo.publisher;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@SpringBootTest
@EnableRabbit
public class PublisherReturns {

    @Autowired
    private RabbitTemplate rabbitTemplate;


    @Configuration
    public static class Config {
        @Bean
        public ConnectionFactory connectionFactory() {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
            connectionFactory.setPublisherReturns(true);
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
    public void testConfirm() {
        rabbitTemplate.setReturnsCallback(returned -> {
            log.error("Message returned: " + returned.getMessage());
            log.error("Reason: " + returned.getReplyText());
            log.error("Reply code: " + returned.getReplyCode());
            log.error("Exchange: " + returned.getExchange());
            log.error("Routing key: " + returned.getRoutingKey());
        });
       rabbitTemplate.convertAndSend("myQueue1", "foo");
    }


}
