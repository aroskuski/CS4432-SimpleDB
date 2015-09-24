package simpledb.buffer;

/*CS4432-Project1 This file is the interface that both replacement
 * policies are based on.*/
public interface ReplacementPolicy {
	
	/*CS4432-Project1 Fills the array with the neutral number so that
	 * the buffer at that index can be denoted as empty*/
	public void fillArray(int NumBuffers);
	
	/*CS4432-Project1 Gets the index of the buffer that is to be replaced*/
	public int indexToReplace();
	
	/*CS4432-Project1 Notes that the buffer being added is pinned and
	 * sets a value according to it.*/
	public void newPin(Buffer buff);

	/*CS4432-Project1 Notes that the buffer at this index is pinned*/
	public void pin(Buffer buff);
	
	/*CS4432-Project1 Notes that the buffer at this index is no longer
	 * pinned.*/
	public void unpin(Buffer buff);
}
