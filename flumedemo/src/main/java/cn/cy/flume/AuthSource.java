package cn.cy.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Map;

public class AuthSource extends AbstractSource implements EventDrivenSource, Configurable {
    private int step=1;

    @Override
    public void configure(Context context) {

    }

    @Override
    public synchronized void start() {

    }

    @Override
    public synchronized void stop() {
        super.stop();
    }
}

class Increment implements Runnable{
    private int step;

    public void setStep(int step) {
        this.step = step;
    }

    @Override
    public void run() {
        int i=0;
        while(true){
            //每次产生一个数字,这个数字都要作为数据进行收集
            //在flume中,数据是以Event来传递的
            //就需要将数据封装成Event形式
            Map<String,String> headers=new HashMap<>();
            byte[] body=(i+" ").getBytes();
            Event e= EventBuilder.withBody(body,headers);


            i+=step;

        }
    }
}
