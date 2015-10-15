package simpledb.index.hash;

import simpledb.query.Constant;
import simpledb.query.TableScan;
import simpledb.record.RID;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

public class ExtensibleHashBucket {
	private static final int MAX_ENTRIES = 30;
	private TableInfo ti;
	private Transaction tx;
	private Constant searchkey = null;
	private TableScan ts = null;
	
	public ExtensibleHashBucket(String bucketName, Schema bucketSch, Transaction tx) {
		this.ti = new TableInfo(bucketName, bucketSch);
		this.tx = tx;

	}
	
	public void beforeFirst(Constant searchkey) {
		close();
		this.searchkey = searchkey;
		ts = new TableScan(ti, tx);
	}
	
	public boolean next() {
		while (ts.next()){
			if (ts.getVal("dataval").equals(searchkey)){
				return true;
			}
		}
		
		return false;
	}
	
	public RID getDataRid() {
		int blknum = ts.getInt("block");
		int id = ts.getInt("id");
		return new RID(blknum, id);
	}
	
	public void insert(Constant val, RID rid) {
		beforeFirst(val);
		ts.insert();
		ts.setInt("block", rid.blockNumber());
		ts.setInt("id", rid.id());
		ts.setVal("dataval", val);
	}
	
	public void delete(Constant val, RID rid) {
		beforeFirst(val);
		while(next()){
			if (getDataRid().equals(rid)) {
				ts.delete();
				return;
			}
		}
	}
	
	public void close(){
		if (ts != null){
			ts.close();
		}
	}
}
