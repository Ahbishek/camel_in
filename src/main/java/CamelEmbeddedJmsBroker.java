import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.impl.DefaultCamelContext;

import javax.jms.ConnectionFactory;




public final class CamelEmbeddedJmsBroker {

    private CamelEmbeddedJmsBroker() {}

    public static void main(String args[]) throws Exception {
        CamelContext context = new DefaultCamelContext();

        /* 1. vm embedded broker.
        Broker is started when first connection is created using vm protocol .
        Hence over here  we don't explicitly create another broker.

        The VM transport allows clients to connect to each other inside the VM without the overhead of the network communication.
        The connection used is not a socket connection but use direct method invocations which enables a high performance embedded messaging system.
        The first client to use the VM connection will boot an embedded broker.
        Subsequent connections will attach that the same broker.
        Once all VM connections to the broker have been closed, the embedded broker will automatically shutdown.
        */

        /*Create basic vm broker using following
                 ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm://localhost?broker.persistent=false");
       */


        /* Create a vm broker . broker:(tcp://localhost:60000) enables connection to this embedded broker using tcp protocl. This way (tcp) of connection can be used from  jms-component outside this jvm */
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "vm:broker:(tcp://localhost:60000)?persistent=true");
        /* One can set broker-name  by adding following attribute: brokerName=TeamIntegrationBroker&*/


        /*  If user wants to connect to already existing  broker use the following . Note: Broker must first start the  MOM at localhost:61616
                ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "tcp://localhost:61616");
        */


        /*
        *Create a jms component 'test-jms'
        *  AutoAcknowledgement Connection-Session is created.
        * */
        context.addComponent("test-jms",
                JmsComponent.jmsComponentAutoAcknowledge(connectionFactory));


        createCamelRoute(context);
        sendMessageOnQueue(context);
    }

    private static void createCamelRoute(CamelContext context) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("test-jms:queue:test.queue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("Message " + exchange.getIn().getBody());
                    }
                }).to("file://C:\\Camel-Works\\Code\\output.txt");
            }
        });
    }

    private static void sendMessageOnQueue(CamelContext context) throws Exception {
        ProducerTemplate template = context.createProducerTemplate();
        context.start();
        for (int i = 0; i < 10; i++) {
                    // send data to the queue.
                    template.sendBody("test-jms:queue:test.queue", "Test Message: " + i);
        }
    }
}