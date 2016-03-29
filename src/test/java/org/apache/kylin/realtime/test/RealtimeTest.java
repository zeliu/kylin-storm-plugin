package org.apache.kylin.realtime.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Put;
import org.apache.kylin.realtime.RealTimeCubing;
import org.junit.Test;

/**
 * Created by liuze on 2016/3/24 0024.
 */
public class RealtimeTest {

    @Test
    public void testRealtime() throws Exception {

        Configuration conf = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(conf);
        RealTimeCubing realTimeCubing=new RealTimeCubing(connection,"realtimetest","20160301000000_20160331000000",",");

        String message="1,2016-03-25,1000907,34,67.78";
        List<String> data=new ArrayList<String>();

        String d1="1,2016-03-26,1000907,100,456.78";
        String d2="2,2016-03-26,1000964,123,123.78";
        data.add(d1);
        data.add(d2);
        List<Put> puts=new ArrayList<Put>();
        //for one message
        //realTimeCubing.createPut(message,puts);
        // for batch
        realTimeCubing.createPut(data, puts);
        realTimeCubing.commitPut(puts);




    }
}
