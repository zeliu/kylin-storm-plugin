package org.apache.kylin.realtime;

import org.apache.kylin.common.util.Bytes;

/**
 * Created by liuze on 2016/3/28 0028.
 */
public class StreamingData {
    private byte[] rowkey;
    private Object[] values;
    private String key;

    public StreamingData(byte[] rowkey, Object[] values) {
        this.rowkey = rowkey;
        this.values = values;
        this.key= Bytes.toStringBinary(rowkey);
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getRowkey() {
        return rowkey;
    }

    public void setRowkey(byte[] rowkey) {
        this.rowkey = rowkey;
    }

    public Object[] getValues() {
        return values;
    }

    public void setValues(Object[] values) {
        this.values = values;
    }


}
