import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.JndiRegistry;
import org.springframework.jms.connection.JmsTransactionManager;

import javax.jms.ConnectionFactory;


public final class CamelJmsTransactionRedeliveryDeadLetterQueueExample {

    private CamelJmsTransactionRedeliveryDeadLetterQueueExample() {}

    public static void main(String args[]) throws Exception {
        JndiRegistry registry = new JndiRegistry();

        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm:broker:(tcp://localhost:60000)?persistent=true");

        JmsTransactionManager jmsTxManager = new JmsTransactionManager();
        jmsTxManager.setConnectionFactory(connectionFactory);
        registry.bind("jmsTxManager", jmsTxManager);
        CamelContext context = new DefaultCamelContext(registry);


        context.addComponent("test-jms",
                JmsComponent.jmsComponentTransacted(connectionFactory,jmsTxManager));


        createCamelQueue(context);
        sendMessageOnQueue(context);
    }

    private static void createCamelQueue(CamelContext context) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {

                errorHandler(deadLetterChannel("test-jms:queue:dlq").maximumRedeliveries(2)
                        .redeliveryDelay(100).useOriginalMessage());
                //useOriginalMessage()

                from("test-jms:queue:test.queue").transacted().process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Message Received " + exchange.getIn().getBody());
                    }
                }).process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                       System.out.println("In Processor Number 2");

                       //To Test Exception and Dead Letter Channel adding following exception.
                       throw  new  Exception("Some Exception");
                    }
                });


                from("test-jms:queue:dlq").transacted().process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Reading from Dead Letter Queue :  Message Received " + exchange.getIn().getBody());
                    }
                });

            }
        });



    }

    private static void sendMessageOnQueue(CamelContext context) throws Exception {

        printTransactionAndDeadLetterChannelConcept();

        ProducerTemplate template = context.createProducerTemplate();
        context.start();
        System.out.println("*********************************************");
        System.out.println("CamelRequestReplyRoute Using ExclusiveQueue");
        System.out.println("*********************************************\n");
        template.sendBody("test-jms:queue:test.queue", "Test Message 1 ");
    }

    private static void printTransactionAndDeadLetterChannelConcept(){
        System.out.println("Transactions\n" +
                "\n" +
                "When an exchange enters the transacted processor, the transacted processor invokes the default transaction manager to begin a transaction and attaches the transaction to the current thread.\n" +
                "When the exchange reaches the end of the remaining route, the transacted processor invokes the transaction manager to commit the current transaction.\n" +
                "The transaction is associated with only the current thread.\n" +
                "The transaction scope encompasses all of the route nodes that follow the transacted processor.\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "\n" +
                "Redelivery and Dead Letter Queue;\n" +
                "\n" +
                "Messages can be delivered unsuccessfully (e.g. if the transacted session used to consume them is rolled back). Such a message goes back to its queue ready to be redelivered. However, this means it is possible for a message to be delivered again and again without success thus remaining in the queue indefinitely, clogging the system.\n" +
                "\n" +
                "There are 2 ways to deal with these undelivered messages:\n" +
                "\n" +
                "Delayed redelivery.\n" +
                "\n" +
                "It is possible to delay messages redelivery. This gives the client some time to recover from any transient failures and to prevent overloading its network or CPU resources.\n" +
                "\n" +
                "Dead Letter Address.\n" +
                "It is also possible to configure a dead letter address so that after a specified number of unsuccessful deliveries, messages are removed from their queue and sent to the dead letter address. These messages will not be delivered again from this queue.");
    }
}