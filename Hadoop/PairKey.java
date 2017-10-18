import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
//writing a class for pairkey
public class PairKey implements WritableComparable<PairKey>{
	private String businessid;
	private int source;
	//method for reading
	public void readFields(DataInput in) throws IOException {
		businessid=in.readUTF();
		source=in.readInt();
	}
	public void write(DataOutput out) throws IOException{
		out.writeUTF(businessid);
		out.writeInt(source);
	}
	public int getSource(){
		return this.source;
	}
	public String getId(){
		return this.businessid;
	}
	public void set(String businessid, int src){
		this.businessid = businessid;
		this.source = src;
	}
	public int compareTo(PairKey o){
		if(this.source!=o.getSource()){
			return this.source-o.getSource();
		}
		else{
			return this.businessid.compareTo(o.getId());
		}
	}
}