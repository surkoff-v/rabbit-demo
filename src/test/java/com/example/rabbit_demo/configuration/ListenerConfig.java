package com.example.rabbit_demo.configuration;

import com.example.rabbit_demo.listeners.FooBarListener;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RabbitListenerTest
public class ListenerConfig {
    @Bean
    public FooBarListener fooBarListener() {
        return new FooBarListener();
    }
    @Bean
    public Queue queue1() {
        return new AnonymousQueue();
    }

    @Bean
    public Queue queue2() {
        return new AnonymousQueue();
    }


}
