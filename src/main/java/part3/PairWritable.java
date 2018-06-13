package part3;

/**
 * Created by Wenqiang on 6/13/18.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class PairWritable implements WritableComparable<PairWritable>{

    private Text key;
    private Text value;

    public PairWritable(){
        key = new Text();
        value = new Text();
    }

    public PairWritable(String key, String value){
        setKey(new Text(key));
        setValue(new Text(value));
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        // TODO Auto-generated method stub
        key.readFields(input);
        value.readFields(input);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        // TODO Auto-generated method stub
        key.write(output);
        value.write(output);
    }

    public String toString() {
        return "("+this.key+", "+this.value+")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.key.toString(),this.value.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PairWritable))
            return false;
        PairWritable other = (PairWritable)obj;
        return this.key.toString().equals(other.key.toString()) && this.value.toString().equals(other.value.toString());
    }

    @Override
    public int compareTo(PairWritable o) {
        // TODO Auto-generated method stub
        int k  = this.key.toString().compareTo(o.key.toString());
        if (k!=0) return k;
        else return this.value.toString().compareTo(o.value.toString());

    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key = key;
    }

    public Text getValue() {
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }


}
