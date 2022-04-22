package mao;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import mao.tools.RabbitMQ;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

/**
 * Project name(项目名称)：rabbitMQ消息发布确认之异步确认发布
 * Package(包名): mao
 * Class(类名): Producer
 * Author(作者）: mao
 * Author QQ：1296193245
 * GitHub：https://github.com/maomao124/
 * Date(创建日期)： 2022/4/22
 * Time(创建时间)： 19:05
 * Version(版本): 1.0
 * Description(描述)：
 * <p>
 * 1000条：51.859毫秒
 * 速度:74429/s
 */

public class Producer
{
    private static final String QUEUE_NAME = "work";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException
    {
        Channel channel = RabbitMQ.getChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        //在此信道上启用发布者确认
        channel.confirmSelect();
        //线程安全有序的一个哈希表，适用于高并发的情况
        ConcurrentSkipListMap<Long, String> concurrentSkipListMap = new ConcurrentSkipListMap<>();

        //添加一个异步确认的监听器
        channel.addConfirmListener(new ConfirmCallback()
        {
            @Override
            public void handle(long deliveryTag, boolean multiple) throws IOException
            {
                if (multiple)
                {
                    //返回的是小于等于当前序列号的未确认消息 是一个 map
                    //ConcurrentNavigableMap:一个支持NavigableMap操作的ConcurrentMap ，并且递归地支持其可导航的子地图。
                    ConcurrentNavigableMap<Long, String> confirmed = concurrentSkipListMap.headMap(deliveryTag, true);
                    //清除该部分未确认消息
                    confirmed.clear();
                }
                else
                {
                    concurrentSkipListMap.remove(deliveryTag);
                }
            }
        }, new ConfirmCallback()
        {
            @Override
            public void handle(long deliveryTag, boolean multiple) throws IOException
            {
                String message = concurrentSkipListMap.get(deliveryTag);
                System.out.println("发布的消息" + message + "未被确认，序列号" + deliveryTag);
            }
        });


        //------------------------------------------------------
        long startTime = System.nanoTime();   //获取开始时间
        //------------------------------------------------------
        for (int i = 0; i < 1000; i++)
        {
            String message = "消息" + (i + 1);
            concurrentSkipListMap.put(channel.getNextPublishSeqNo(), message);
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes(StandardCharsets.UTF_8));

        }
        //------------------------------------------------------
        long endTime = System.nanoTime(); //获取结束时间
        if ((endTime - startTime) < 1000000)
        {
            double final_runtime;
            final_runtime = (endTime - startTime);
            final_runtime = final_runtime / 1000;
            System.out.println("算法运行时间： " + final_runtime + "微秒");
        }
        else if ((endTime - startTime) >= 1000000 && (endTime - startTime) < 10000000000L)
        {
            double final_runtime;
            final_runtime = (endTime - startTime) / 1000;
            final_runtime = final_runtime / 1000;
            System.out.println("算法运行时间： " + final_runtime + "毫秒");
        }
        else
        {
            double final_runtime;
            final_runtime = (endTime - startTime) / 10000;
            final_runtime = final_runtime / 100000;
            System.out.println("算法运行时间： " + final_runtime + "秒");
        }
        Runtime r = Runtime.getRuntime();
        float memory;
        memory = r.totalMemory();
        memory = memory / 1024 / 1024;
        System.out.printf("JVM总内存：%.3fMB\n", memory);
        memory = r.freeMemory();
        memory = memory / 1024 / 1024;
        System.out.printf(" 空闲内存：%.3fMB\n", memory);
        memory = r.totalMemory() - r.freeMemory();
        memory = memory / 1024 / 1024;
        System.out.printf("已使用的内存：%.4fMB\n", memory);
        //------------------------------------------------------

        System.out.println("消息全部发送完成");
    }
}
