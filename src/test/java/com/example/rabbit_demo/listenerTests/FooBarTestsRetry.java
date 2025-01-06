package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.amqp.rabbit.test.RabbitListenerTestHarness.InvocationData;

@SpringBootTest
@Slf4j
public class FooBarTestsRetry {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest(spy = true, capture = false)
    public static class ListenerConfig {
        @Bean
        public Listener listener() {
            return new Listener();
        }

        @Bean
        public ConnectionFactory connectionFactory() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        public Queue queue1() {
            return new AnonymousQueue();
        }

        @Bean
        public Queue queue2() {
            return new AnonymousQueue();
        }

        @Bean
        public RabbitTemplate template(ConnectionFactory cf) {
            RabbitTemplate template = new RabbitTemplate(connectionFactory());
            RetryTemplate retryTemplate = new RetryTemplate();
            // recovery callback will be user for retry template
            template.setRecoveryCallback(context -> {
                int retryCount = context.getRetryCount();
                context.getLastThrowable().getStackTrace();
                log.error("Retry count: " + retryCount  + " " + context.getLastThrowable().getMessage());
                return null;
            });
            ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
            backOffPolicy.setInitialInterval(500); // halh of the second
            backOffPolicy.setMultiplier(10.0); // interval will be multiplied by 10 for the next attempt
            backOffPolicy.setMaxInterval(10000); // max interval is 10 seconds
            retryTemplate.setBackOffPolicy(backOffPolicy);
            template.setRetryTemplate(retryTemplate);
            template.setUsePublisherConnection(true);
            return template;
        }

        @Bean
        public RabbitAdmin admin(ConnectionFactory cf) {
            return new RabbitAdmin(cf);
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf) {
            SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();
            containerFactory.setConnectionFactory(cf);
            return containerFactory;
        }
    }

    @Autowired
    private RabbitListenerTestHarness harness;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    private Queue queue1;

    @Autowired
    private Queue queue2;

    @Test
    public void testTwoWay() throws Exception {
        assertEquals("FOO", this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo"));

        InvocationData invocationData =
                this.harness.getNextInvocationDataFor("foo", 0, TimeUnit.SECONDS);
        assertThat(invocationData.getArguments()[0]).isEqualTo("foo");
        assertThat((String) invocationData.getResult()).isEqualTo("FOO");
    }

    @Test
    public void testOneWay() throws Exception {
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "bar");
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "baz");
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "ex");

        InvocationData invocationData =
                this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        Object[] args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("bar");
        assertThat((String) args[1]).isEqualTo(queue2.getName());

        invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("baz");

        invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("ex");
        assertEquals("ex", invocationData.getThrowable().getMessage());
    }


   public static class Listener {
       private boolean failed;

       @RabbitListener(id="foo", queues="#{queue1.name}")
       public String foo(String foo) {
           return foo.toUpperCase();
       }

       @RabbitListener(id="bar", queues="#{queue2.name}")
       public void foo(@Payload String foo, @Header("amqp_receivedRoutingKey") String rk) {
           if (!failed && foo.equals("ex")) {
               failed = true;
               throw new RuntimeException(foo);
           }
           failed = false;

       }
   }

}
