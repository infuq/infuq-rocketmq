package org.apache.rocketmq.example.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class Consumer {


    public static void main(String[] args) throws MQClientException {

        //System.out.println(System.getProperty("enablePrint"));

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_CONSUMER_ORDER");
        consumer.setConsumeTimeout(2);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr("192.168.20.36:8876");
        consumer.subscribe("ORDER_FINISH", "*");
        consumer.subscribe("ORDER_UPDATE", "*");

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


        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

}
