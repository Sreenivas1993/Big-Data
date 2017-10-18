import org.apache.hadoop.io.Writable;
import java.io.*;
public class PairWritable implements Writable {
	private float star;
	private int count;
	public PairWritable(){
		
	}
	public void write(DataOutput out)throws IOException{
		out.writeFloat(star);
		out.writeInt(count);
	}
	public void readFields(DataInput in)throws IOException{
		star=in.readFloat();
		count=in.readInt();
	}
	public float getStar(){
		return star;
	}
	public int getCount(){
		return count;
	}
	public void set(float star,int count){
		this.count=count;
		this.star=star;
	}
}
