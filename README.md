# kylin-storm-plugin

        Example:


        First, build a cube, and cancel all Field Dictionary (if you use multithreading , can put the thread ID as a dimension)



        Configuration conf = HBaseConfiguration.create();
        HConnection connection = HConnectionManager.createConnection(conf);
        //Hconnection   cubename   segment
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

