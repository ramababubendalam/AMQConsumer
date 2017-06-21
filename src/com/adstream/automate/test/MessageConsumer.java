package com.adstream.automate.test;

/**
 * Created by Ramababu.Bendalam on 18/06/2017.
 */
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MessageConsumer extends AbstractJavaSamplerClient implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final static Logger log = LoggerFactory.getLogger(MessageConsumer.class);
    private long defaultTimeout = 3*60*1000;


    // set up default arguments for the JMeter GUI
    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument("queueName", "adstream.yadn.response.lno1v-a5dapp08Test");
        defaultParameters.addArgument("url", "tcp://10.44.127.90:61616");
        defaultParameters.addArgument("response", "UploadCompleteJobResponse");
        defaultParameters.addArgument("fileID","");
        return defaultParameters;
    }

   @Override
    public SampleResult runTest(JavaSamplerContext context) {
        // pull parameters

        String url = context.getParameter("url");
        String queueName = context.getParameter("queueName");
        String response = context.getParameter("response");
        String fileID  = context.getParameter("fileID");
        SampleResult result = new SampleResult();
        result.sampleStart(); // start stopwatch

        MessageConsumer consumer1 = new MessageConsumer();
        try {
            consumer1.run(url, queueName, response, fileID);
            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( true );
            result.setResponseMessage( "Successfully performed action" );
            result.setResponseCodeOK(); // 200 code
        } catch (NamingException e) {
            e.printStackTrace();
            result.sampleEnd(); // stop stopwatch
            result.setSuccessful( false );
            result.setResponseMessage( "Exception: " + e );

            // get stack trace as a String to return as document data
            java.io.StringWriter stringWriter = new java.io.StringWriter();
            e.printStackTrace( new java.io.PrintWriter( stringWriter ) );
            result.setResponseData( stringWriter.toString() );
            result.setDataType( org.apache.jmeter.samplers.SampleResult.TEXT );
            result.setResponseCode( "500" );
        } catch (JMSException e) {
            e.printStackTrace();
        }

        return result;
    }

    public MessageConsumer() {
     }

    public List<String> run(String url, String queueName, String response, String fileID) throws NamingException, JMSException {
        //JNDI properties
        Properties props = new Properties();
        props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        props.setProperty(Context.PROVIDER_URL, url);
        //specify queue propertyname as queue.jndiname
        props.setProperty("queue.slQueue", queueName);
        javax.naming.Context ctx = new InitialContext(props);
        Destination destination = (Destination) ctx.lookup("slQueue");
        MessageFilter mf = new MessageFilter();

        return mf.consume(1, defaultTimeout, false, true, url,destination, response, fileID);

    }








}
