package org.apache.hadoop.mapred;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ganyi on 9/29/2016.
 */
public class ReduceProgressMessage implements Writable {

    private int type;
    //private String typeStr;

    private int partitionId;
    private int reduceId;

    public ReduceProgressMessage(){}

    public ReduceProgressMessage(String t){
        //assert t=="request" || t == "respond";
        if(t=="request"){
            type = 1;
        }
        else if(t == "respond"){
            type = 2;
        }
        else{
            //report Error;
        }
    }


    public void setPartitionId(int i){partitionId = i;}
    public void setReduceId(int i){reduceId=i;}
    public int getPartitionId(){return partitionId;}
    public int getReduceId(){return reduceId;}

    public String getType(){
        return type==1?"request":"respond";
    }
    public void setType(String str){type = (str=="request"?1:2);}

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(type==1?"Q":"R").append("<").append(reduceId).append(",").append(partitionId).append(">");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(type);
        out.writeInt(partitionId);
        out.writeInt(reduceId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        type = in.readInt();
        partitionId = in.readInt();
        reduceId = in.readInt();
    }
}
