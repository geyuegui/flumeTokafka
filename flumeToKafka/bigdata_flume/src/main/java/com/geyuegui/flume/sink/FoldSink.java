package com.geyuegui.flume.sink;

import com.geyuegui.com.producer.LogProducer;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

public class FoldSink extends AbstractSink implements Configurable {
    private static final Logger LOG = Logger.getLogger(FoldSink.class);
    private String myProp;
    private String topic;

    @Override
    public void configure(Context context) {
        topic = context.getString("topics");
    }

    @Override
    public void start() {
        // Initialize the connection to the external repository (e.g. HDFS) that
        // this Sink will forward Events to ..
    }

    @Override
    public void stop () {
        // Disconnect from the external respository and do any
        // additional cleanup (e.g. releasing resources or nulling-out
        // field values) ..
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        LOG.info("============txt begin============");
        txn.begin();
        LOG.info("============txt begin after============");
        try {
            // This try clause includes whatever Channel operations you want to do

            Event event = ch.take();
            LOG.info("============Event event = ch.take();============");
            LOG.info("topic:"+topic);
            LOG.info("event:"+(event==null));
//            LOG.info("event:"+new String(event.getBody(),"UTF-8"));
            if(event==null){
                LOG.info("============txn.rollback();============");
                txn.rollback();
                LOG.info("============sink end;============");
                return Status.BACKOFF;
            }
            String line =new String(event.getBody());
            LOG.info("==="+line);
            LogProducer.producer(topic,line);
            LOG.info("==="+line);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }

}
