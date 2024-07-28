package org.apache.rocketmq.example.quickstart;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

public class ConcurrentConsumer {

    public static void main(String[] args) throws MQClientException {

        System.setProperty("enablePrint", "0");

        String nameSrvAddr = "192.168.31.130:9876";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_CONSUMER_ORDER");
        consumer.setConsumeTimeout(2);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setNamesrvAddr(nameSrvAddr);
        consumer.subscribe("ORDER_FINISH", "*");
        consumer.subscribe("ORDER_UPDATE", "*");

        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                System.out.println("[" + Thread.currentThread().getName() + "]调用程序员方法被休眠,模拟执行耗时操作");
                try {
                    Thread.sleep(3600 * 1000);
                } catch (Exception e) {}
                System.out.println("[" + Thread.currentThread().getName() + "]该批次消息数量=" + msgs.size());
                for (MessageExt v : msgs) {
                    System.out.println("[" + Thread.currentThread().getName() + "]处理消息: " + new String(v.getBody()) + ", " + JSON.toJSONString(v.getStoreHost()) + ", msgId=" + v.getMsgId() + ", queueId=" + v.getQueueId() + ", tag=" + v.getTags() + ", topic=" + v.getTopic());
                }

                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                //return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        });


        consumer.start();

        System.out.printf("ConcurrentConsumer Started.%n");
    }

}
