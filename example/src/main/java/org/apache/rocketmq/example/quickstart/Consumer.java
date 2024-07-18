package org.apache.rocketmq.example.quickstart;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

public class Consumer {

    private static volatile int index = 1;
    private static final Lock lock = new ReentrantLock();

    public static void main(String[] args) throws MQClientException {

        //System.out.println(System.getProperty("enablePrint"));

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_CONSUMER_ORDER_PAY");
        consumer.setConsumeTimeout(2);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr("192.168.20.36:8876");
        consumer.subscribe("ORDER_FINISH", "*");
        consumer.subscribe("ORDER_UPDATE", "*");
boolean f = true;
if (f) {
    consumer.registerMessageListener(new MessageListenerConcurrently() {

        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

            //System.out.println("[" + Thread.currentThread().getName() + "]该批次消息数量=" + msgs.size());
            for (MessageExt v : msgs) {
                System.out.println("[" + Thread.currentThread().getName() + "]处理消息: " + new String(v.getBody()) + ", " + JSON.toJSONString(v.getStoreHost()) + ", msgId=" + v.getMsgId() + ", queueId=" + v.getQueueId() + ", tag=" + v.getTags() + ", topic=" + v.getTopic());
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        }
    });
}
else {
    consumer.registerMessageListener(new MessageListenerOrderly() {
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            System.out.println("[" + Thread.currentThread().getName() + "]该批次消息数量=" + msgs.size());
            for (MessageExt v : msgs) {
                System.out.println("[" + Thread.currentThread().getName() + "]处理消息: " + JSON.toJSONString(v.getStoreHost()) + ", msgId=" + v.getMsgId() + ", queueId=" + v.getQueueId());
            }
            return ConsumeOrderlyStatus.SUCCESS;
        }
    });
}

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

}
