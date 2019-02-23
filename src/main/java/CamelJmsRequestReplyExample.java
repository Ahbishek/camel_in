import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import pojo.Message;

import javax.jms.ConnectionFactory;
import java.util.UUID;


public final class CamelJmsRequestReplyExample {

    private CamelJmsRequestReplyExample() {}

    public static void main(String args[]) throws Exception {
        CamelContext context = new DefaultCamelContext();



        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm:broker:(tcp://localhost:60000)?persistent=true");

        context.addComponent("test-jms",
                JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));


        /*Camel uses
        1. By default if JMSReplyTo is not specified, camel uses temp queue.
        2. If JMSReplyTo is specified and that queue is not exclusive. Camel uses selectors to select messages.Tends to be slow. Only those messages having same correlationID are used from that queue
        3. If JMSReplyTo is specified and that queue is  exclusive. Camel  do not use selectors to select messages.
         */

        createCamelRequestReplyRouteUsingExclusiveQueue(context);

        createCamelRequestReplyRouteUsingTempQueue(context);
        sendMessageOnQueue(context);



    }

    private static void createCamelRequestReplyRouteUsingExclusiveQueue(CamelContext context) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("test-jms:queue:test.queue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Message Received " + exchange.getIn().getBody());
                        exchange.getOut().setBody("Hi Siri ! Which book should I refer to learn camel?");
                        //Not explicitly setting any jmsCorrelationID.Camel sets an unique id
                        // exchange.getOut().setHeader("JMSCorrelationID", UUID.randomUUID());
                    }
                }).inOut("test-jms:queue:test.siriQueue?replyTo=test.tempQueue?replyToType=Exclusive");

                //You can process the message after inout() too. using  inout(...).process();
               /* inOut("test-jms:queue:test.siriQueue?replyTo=test.tempQueue?replyToType=Exclusive") .process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("From tempQueue .Reply Message JMSCorrelationID : " + exchange.getIn().getHeader("JMSCorrelationID"));
                        System.out.println("From tempQueue .Reply Message received from siriQueue  : " + exchange.getIn().getBody());
                    }
                });
                */
                // IF replyToType=Exclusive is not used then , tempQueue is treated as shared queue. JMS Selectors are used then which only passes those exchange object to the next .process()  if the correlationID matches.Hence it tends to be slow.

                from("test-jms:queue:test.siriQueue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Request to Siri");
                        System.out.println("JMSCorrelationID : " + exchange.getIn().getHeader("JMSCorrelationID"));
                        System.out.println("Message: " + exchange.getIn().getBody());
                        exchange.getOut().setBody(exchange.getIn().getBody() + " CAMEL IN ACTION");
                    }
                });


                from("test-jms:queue:test.tempQueue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Reply from Siri on tempQueue");
                        System.out.println("JMSCorrelationID : " + exchange.getIn().getHeader("JMSCorrelationID"));
                        System.out.println("Message: " + exchange.getIn().getBody());

                    }
                });
            }
        });



    }

    private static void createCamelRequestReplyRouteUsingTempQueue(CamelContext context) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("test-jms:queue:test.initQueue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Message Received " + exchange.getIn().getBody());
                        exchange.getOut().setBody("Hi Siri ! Which book should I refer to learn activeMQ?");
                        exchange.getOut().setHeader("JMSCorrelationID", UUID.randomUUID());
                    }
                }).inOut( "test-jms:queue:test.newSiriQueue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Reply from Siri");
                        System.out.println("JMSCorrelationID : " + exchange.getIn().getHeader("JMSCorrelationID"));
                        System.out.println("Message: " + exchange.getIn().getBody());
                    }
                });


                // Using wildcard concept.
                from("test-jms:queue:*.newSiriQueue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Request to Siri");
                        System.out.println("JMSCorrelationID : " + exchange.getIn().getHeader("JMSCorrelationID"));
                        System.out.println("Message: " + exchange.getIn().getBody());
                        Message message =new Message(exchange.getIn().getBody() + "ActiveMQ IN ACTION");
                        exchange.getIn().setBody(message);
                       // exchange.getOut().setBody(exchange.getIn().getBody() + " ActiveMQ IN ACTION");
                    }
                });

            }
        });



    }


    private static void sendMessageOnQueue(CamelContext context) throws Exception {
        ProducerTemplate template = context.createProducerTemplate();
        context.start();
        System.out.println("*********************************************");
        System.out.println("CamelRequestReplyRoute Using ExclusiveQueue");
        System.out.println("*********************************************\n");
        template.sendBody("test-jms:queue:test.queue", "Test Message 1 ");
        Thread.sleep(3000);
        System.out.println("\n*********************************************");
        System.out.println("CamelRequestReplyRoute Using TempQueue");
        System.out.println("*********************************************\n");
        template.sendBody("test-jms:queue:test.initQueue", "Test Message 2 ");


    }
}