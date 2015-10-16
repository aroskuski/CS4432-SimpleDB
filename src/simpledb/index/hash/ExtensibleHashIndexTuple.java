package simpledb.index.hash;

/*CS4432 Describes a tuple in the extensible has*/
public class ExtensibleHashIndexTuple {
	public final int hash;
	public final String bucket;
	
	/*CS4432 Creates the tuple and the hash and bucket it is in*/
	ExtensibleHashIndexTuple(int hash, String bucket){
		this.hash = hash;
		this.bucket = bucket;
	}

}
