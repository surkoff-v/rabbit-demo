package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@SpringBootTest
public class ProgrammaticalListenerConfiguration {

    @Configuration
    public static class conf{
        @Bean
        CachingConnectionFactory connectionFactory() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        RabbitTemplate rabbitTemplate() {
            return new RabbitTemplate(connectionFactory());
        }

        @Bean
        Queue queue() {
            return new Queue("dynamicQueue");
        }

        @Bean
        RabbitAdmin rabbitAdmin() {
            return new RabbitAdmin(connectionFactory());
        }
    }

    @Autowired
    CachingConnectionFactory connectionFactory;

    @Autowired
    RabbitTemplate rabbitTemplate;


    private SimpleMessageListenerContainer createContainer() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames("dynamicQueue");
        container.setMessageListener(exampleListener());
        container.start();
        return container;
    }

    public MessageListener exampleListener() {
            return message -> System.out.println("received: " + message);
    }

    @Test
    public void testDynamicQueue() {
        SimpleMessageListenerContainer container = createContainer();
        rabbitTemplate.convertAndSend("dynamicQueue", "Hello, World!");
        container.stop();
    }

}
