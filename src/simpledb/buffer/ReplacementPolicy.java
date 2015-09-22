package simpledb.buffer;

public interface ReplacementPolicy {
	
	public void fillArray(int NumBuffers);
	
	public int indexToReplace();
	
	public void newPin(int BuffIndex);

	public void pin(int BuffIndex);
	
	public void unpin(int BuffIndex);
}
