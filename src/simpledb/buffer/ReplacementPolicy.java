package simpledb.buffer;

public interface ReplacementPolicy {
	
	public void fillArray(int NumBuffers);
	
	public int indexToReplace();
	
	public void newPin(Buffer buff);

	public void pin(Buffer buff);
	
	public void unpin(Buffer buff);
}
