# kylin-storm-plugin

   Example:


   First, build a cube, and cancel all Field Dictionary (if you use multithreading , can put the thread ID as a dimension
   and make it Mandatory)


   public class TestBolt extends BaseBasicBolt {
    private RealTimeCubing realTimeCubing;
    private int thisTaskId;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.thisTaskId = context.getThisTaskId();
        Configuration conf = HBaseConfiguration.create();
        try {
            HConnection connection = HConnectionManager.createConnection(conf);
            //Hconnection   cubename   segment
            this.realTimeCubing = new RealTimeCubing(connection, "kylin", "20160301000000_20160331000000", ",");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String date = tuple.getStringByField(GatewayLogFlowScheme.FIELD_DATE);
        String host = tuple.getStringByField(GatewayLogFlowScheme.FIELD_HOST);
        String pv='1';
        String message = date + ',' + host + ',' + thisTaskId + ","+pv;


        List<String> data = new ArrayList<String>();
        data.add(message);
        List<Put> puts = new ArrayList<>();
        try {
            realTimeCubing.createPut(data, puts);
            realTimeCubing.commitPut(puts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}

