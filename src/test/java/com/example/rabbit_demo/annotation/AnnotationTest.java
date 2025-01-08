package com.example.rabbit_demo.annotation;

import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.*;

public class AnnotationTest {

    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "auto.headers", autoDelete = "true",
                    arguments = @Argument(name = "x-message-ttl", value = "10000",
                            type = "java.lang.Integer")),
            exchange = @Exchange(value = "auto.headers", type = ExchangeTypes.HEADERS, autoDelete = "true"),
            arguments = {
                    @Argument(name = "x-match", value = "all"),
                    @Argument(name = "thing1", value = "somevalue"),
                    @Argument(name = "thing2")
            })
    )
    public String handleWithHeadersExchange(String foo) {
        return foo;
    }

}
