package org.apache.kylin.realtime;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.hbase.client.*;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.common.util.BytesSplitter;
import org.apache.kylin.common.util.SplittedBytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.common.RowKeySplitter;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.cuboid.CuboidScheduler;
import org.apache.kylin.cube.kv.AbstractRowKeyEncoder;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.hadoop.hive.CubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.measure.MeasureCodec;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;

import com.google.common.collect.Lists;

/**
 * Created by liuze on 2016/3/28 0028.
 */
public class RealTimeCubing {
    public static final byte[] HIVE_NULL = Bytes.toBytes("\\N");
    public static final String familyName="F1";
    public static final String columnName="M";


    private String intermediateTableRowDelimiter=",";
    private byte byteRowDelimiter;
    private Cuboid baseCuboid;
    private CubeInstance cube;
    private CubeDesc cubeDesc;
    private CubeSegment cubeSegment;
    private List<byte[]> nullBytes;


    private CuboidScheduler cuboidScheduler;
    private Object[] measures;
    private byte[][] keyBytesBuf;
    private ByteBuffer valueBuf = ByteBuffer.allocate(RowConstants.ROWVALUE_BUFFER_SIZE);
    private CubeJoinedFlatTableDesc intermediateTableDesc;
    private BytesSplitter bytesSplitter;
    private AbstractRowKeyEncoder rowKeyEncoder;
    private MeasureCodec measureCodec;
    private byte[] keyBuf = new byte[4096];

    private HBaseColumnFamilyDesc[] hBaseColumnFamilyDescs;

    private HConnection hConnection;

    private KylinConfig kylinConfig;

    private List<MeasureDesc> measureDescs;
    private String cubeName;
    private String segmentName;

    private HTable hTable;
    public RealTimeCubing(HConnection hConnection,String cubeName,String segmentName,String intermediateTableRowDelimiter) throws IOException {
        this.hConnection=hConnection;
        kylinConfig=new KylinConfig();
        kylinConfig.setMetadataUrl("hbase");
        this.cubeName=cubeName;
        this.segmentName=segmentName;
        this.intermediateTableRowDelimiter=intermediateTableRowDelimiter;
        setup(kylinConfig);

    }
    protected void setup(KylinConfig config) throws IOException {
        byteRowDelimiter = Bytes.toBytes(intermediateTableRowDelimiter)[0];

        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSegment = cube.getSegment(segmentName, SegmentStatusEnum.READY);
        this.measureDescs=cubeDesc.getMeasures();
        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        intermediateTableDesc = new CubeJoinedFlatTableDesc(cube.getDescriptor(), cubeSegment);

        bytesSplitter = new BytesSplitter(200, 4096);
        rowKeyEncoder = AbstractRowKeyEncoder.createInstance(cubeSegment, baseCuboid);

        measureCodec = new MeasureCodec(cubeDesc.getMeasures());
        measures = new Object[cubeDesc.getMeasures().size()];

        int colCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        keyBytesBuf = new byte[colCount][];

        hBaseColumnFamilyDescs =cubeDesc.getHBaseMapping().getColumnFamily();
        initNullBytes();

        hTable=new HTable(hConnection.getConfiguration(), cubeSegment.getStorageLocationIdentifier());
    }
    public void commitPut(List<Put> puts) throws IOException {

        hTable.put(puts);
    }
    private List<StreamingData> buildStreamingData(List<String> messages){
        int rowkeyColCount = cubeDesc.getRowkey().getRowKeyColumns().length;
        List<StreamingData> streamingDatas=new ArrayList<StreamingData>();
        for(String message:messages){
            Object [] tmpValues= Arrays.copyOfRange(message.split(intermediateTableRowDelimiter), rowkeyColCount, message.split(intermediateTableRowDelimiter).length);
            Object[] values=new Object[tmpValues.length+1];
            values[0]=1;
            System.arraycopy(tmpValues,0,values,1,tmpValues.length);
            bytesSplitter.split(message.getBytes(), message.length(), byteRowDelimiter);
            intermediateTableDesc.sanityCheck(bytesSplitter);
            List<byte[]> rowkeys=new ArrayList<byte[]>();
            byte[] rowKey = buildKey(bytesSplitter.getSplitBuffers());
            rowkeys.add(rowKey);
            getChild(rowkeys,rowKey);
            for (byte [] key:rowkeys){
                StreamingData streamingData=new StreamingData(key,values);
                streamingDatas.add(streamingData);

            }
        }

        return streamingDatas;
    }
    public void createPut(String message,List<Put> puts) throws IOException {
        List<String> messages=new ArrayList<String>();
        messages.add(message);
        createPut(messages,puts);
    }
    public void createPut(List<String> messages,List<Put> puts) throws IOException {

        List<Get> gets =new ArrayList<Get>();
        List<StreamingData> streamingDatas=buildStreamingData(messages);
        Map<String,StreamingData> streamingDataMap=groupByRowkey(streamingDatas);
        streamingDatas.clear();
        for (Map.Entry<String,StreamingData> entry : streamingDataMap.entrySet()) {
            StreamingData streamingData =entry.getValue();
            streamingDatas.add(streamingData);
            Get get = new Get(streamingData.getRowkey());
            gets.add(get);
        }

        Result[] hbaseGet=hTable.get(gets);

        for(Result result:hbaseGet) {
            byte[] hbaseValue = result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
            Object[] resultValue=new Object[cubeDesc.getMeasures().size()];
            if(hbaseValue!=null) {
                ByteBuffer buff = ByteBuffer.wrap(hbaseValue);
                Object[] re = new Object[cubeDesc.getMeasures().size()];
                measureCodec.decode(buff, re);
                for(int i=0;i<re.length;i++)
                {
                    resultValue[i]=re[i].toString();
                }
                StreamingData streamingData=new StreamingData(result.getRow(),resultValue);
                streamingDatas.add(streamingData);
            }

        }

        streamingDataMap.clear();
        streamingDataMap=groupByRowkey(streamingDatas);
        for (Map.Entry<String,StreamingData> entry : streamingDataMap.entrySet()) {
            StreamingData streamingData = entry.getValue();
            buildValue(streamingData.getValues());
            Put put = getPut(streamingData.getRowkey(), hBaseColumnFamilyDescs, valueBuf);
            puts.add(put);
        }

    }
    protected Put getPut(byte[] rowkey, HBaseColumnFamilyDesc[] hBaseColumnFamilyDescs,ByteBuffer valueBuf ) {
        Put put = new Put(rowkey);
        for(HBaseColumnFamilyDesc hBaseColumnFamilyDesc:hBaseColumnFamilyDescs){
            HBaseColumnDesc[] hBaseColumnDescs=hBaseColumnFamilyDesc.getColumns();
            for(HBaseColumnDesc hBaseColumnDesc:hBaseColumnDescs){
                put.add(org.apache.hadoop.hbase.util.Bytes.toBytes(hBaseColumnFamilyDesc.getName()),
                            org.apache.hadoop.hbase.util.Bytes.toBytes(hBaseColumnDesc.getQualifier()),
                            Bytes.copy(valueBuf.array(), 0, valueBuf.position()));
            }
        }
        return put;
    }
    private Map<String,StreamingData> groupByRowkey(List<StreamingData> streamingDatas){
        Map<String,StreamingData> map=new HashMap<String,StreamingData>();
        for(StreamingData sd:streamingDatas){
            if(map.containsKey(sd.getKey())){
                Object[] result=mergeMeasures(sd.getValues(),map.get(sd.getKey()).getValues());
                StreamingData streamingData=new StreamingData(sd.getRowkey(),result);
                map.put(sd.getKey(),streamingData);
            }else{
                map.put(sd.getKey(), sd);

            }

        }
        return map;
    }

    private Object[] mergeMeasures(Object[] src,Object[] tar){
        assert src.length==tar.length;
        Object[] result =new Object[src.length];
        for(int j=0;j<src.length;j++){
            org.apache.kylin.metadata.model.DataType dt=measureDescs.get(j).getFunction().getReturnDataType();
            if(dt.isBigInt()||dt.isInt()||dt.isSmallInt()||dt.isTinyInt()) {
                result[j]=Long.parseLong(src[j].toString())+  Long.parseLong(tar[j].toString());
            }else if(dt.isDecimal()||dt.isDouble()||dt.isFloat()){
                BigDecimal v1;
                if(tar[j] instanceof String)
                {
                    v1=new BigDecimal(tar[j].toString());
                }else{
                    v1=(BigDecimal)tar[j];
                }

                result[j]=new BigDecimal(src[j].toString()).add(v1);//+  .parseDouble(values[j - 1].toString());
            }else{
                result[j]=Long.parseLong(src[j].toString())+  Long.parseLong(tar[j].toString());
            }
        }
        return result;
    }
    private void getChild(List<byte[]> rowkeys,byte[] nowRow){

        RowKeySplitter rowKeySplitter1 = new RowKeySplitter(cubeSegment, 65, 256);
        long cuboidId = rowKeySplitter1.split(nowRow, Bytes.toString(nowRow).length());

        Cuboid parentCuboid = Cuboid.findById(cubeDesc, cuboidId);
        cuboidScheduler = new CuboidScheduler(cubeDesc);
        Collection<Long> myChildren = cuboidScheduler.getSpanningCuboid(cuboidId);

        for (Long child : myChildren) {
            Cuboid childCuboid = Cuboid.findById(cubeDesc, child);
            keyBuf = new byte[4096];
            int keyLength =buildKey(parentCuboid, childCuboid, rowKeySplitter1.getSplitBuffers());
            rowkeys.add(Bytes.copy(keyBuf, 0, keyLength));
            getChild( rowkeys, Bytes.copy(keyBuf, 0, keyLength));
        }

    }
    private byte[] buildKey(SplittedBytes[] splitBuffers) {
        int[] rowKeyColumnIndexes = intermediateTableDesc.getRowKeyColumnIndexes();

        for (int i = 0; i < baseCuboid.getColumns().size(); i++) {
            int index = rowKeyColumnIndexes[i];
            keyBytesBuf[i] = Arrays.copyOf(splitBuffers[index].value, splitBuffers[index].length);
            if (isNull(keyBytesBuf[i])) {
                keyBytesBuf[i] = null;
            }
        }

        return rowKeyEncoder.encode(keyBytesBuf);
    }

    private int buildKey(Cuboid parentCuboid, Cuboid childCuboid, SplittedBytes[] splitBuffers) {

        int offset = 0;

        // cuboid id
        System.arraycopy(childCuboid.getBytes(), 0, keyBuf, offset, childCuboid.getBytes().length);
        offset += childCuboid.getBytes().length;

        // rowkey columns
        long mask = Long.highestOneBit(parentCuboid.getId());
        long parentCuboidId = parentCuboid.getId();
        long childCuboidId = childCuboid.getId();
        long parentCuboidIdActualLength = Long.SIZE - Long.numberOfLeadingZeros(parentCuboid.getId());
        int index = 1; // skip cuboidId
        for (int i = 0; i < parentCuboidIdActualLength; i++) {
            if ((mask & parentCuboidId) > 0) {// if the this bit position equals
                // 1
                if ((mask & childCuboidId) > 0) {// if the child cuboid has this
                    // column
                    System.arraycopy(splitBuffers[index].value, 0, keyBuf, offset, splitBuffers[index].length);
                    offset += splitBuffers[index].length;
                }
                index++;
            }
            mask = mask >> 1;
        }

        return offset;
    }
    private void initNullBytes() {
        nullBytes = Lists.newArrayList();
        nullBytes.add(HIVE_NULL);
        String[] nullStrings = cubeDesc.getNullStrings();
        if (nullStrings != null) {
            for (String s : nullStrings) {
                nullBytes.add(Bytes.toBytes(s));
            }
        }
    }

    private boolean isNull(byte[] v) {
        for (byte[] nullByte : nullBytes) {
            if (Bytes.equals(v, nullByte))
                return true;
        }
        return false;
    }



    private void buildValue(Object[] values) {

        for (int i = 0; i < measures.length; i++) {
            byte[] valueBytes = Bytes.toBytes(values[i].toString());
            measures[i] = measureCodec.getSerializer(i).valueOf(valueBytes);
        }

        valueBuf.clear();
        measureCodec.encode(measures, valueBuf);
    }
}
