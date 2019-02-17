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
/*        Thread.sleep(1000);
        context.stop();*/
    }

    private static void createCamelRoute(CamelContext context) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("test-jms:queue:test.queue").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws Exception {
                        System.out.println("JMSProperty " + exchange.getIn().getHeader("JMSPriority"));
                    }
                }).to("file://C:\\Abhi-WOrk\\Code\\abhi.txt");
            }
        });
    }

    private static void sendMessageOnQueue(CamelContext context) throws Exception {
        ProducerTemplate template = context.createProducerTemplate();
        context.start();
        for (int i = 0; i < 10; i++) {
                    // send data to the queue.
                    template.sendBodyAndHeader("test-jms:queue:test.queue", "Test Message: " + i,"JMSPriority",new Integer(4));
                    //template.sendBody("test-jms:queue:test.queue", "Test Message: " + i);
        }
    }
}