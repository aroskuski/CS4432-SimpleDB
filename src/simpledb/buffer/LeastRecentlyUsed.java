package simpledb.buffer;

import java.util.Arrays;

public class LeastRecentlyUsed implements ReplacementPolicy {
	
	
	/*List of the status of the buffer. The value stored at an
	index represents whether the data is pinned, unpinned, 
	or was used recently. 0 means there's no data, -1 means it
	is pinned, and any other value means that there's unpinned
	data at that location in the buffer. The lower the number,
	the more recently it has been used.*/
	public int[] BufferIndexes;
	
	private Buffer[] bufferPool;
	
	LeastRecentlyUsed(Buffer[] bufferPool){
		this.bufferPool = bufferPool;
	}
	
	/*Creates a clean list of 0s.*/
	public void fillArray(int NumBuffers)
	{
		for (int i=0; i<NumBuffers; i++)
	         BufferIndexes[i] = 0;
		
	}
	
	/*Executes least recently used replacement policy. Uses the list
	to find the largest number in BufferIndexes. The index with the 
	largest number means that the buffer at that location was the
	least recently used buffer. Only to be used if buffer is full.*/
	public int indexToReplace()
	{
		int storedIndex = -1;
		int largestValue = 0;
		for (int i=0; i<BufferIndexes.length; i++){
			if (BufferIndexes[i] > largestValue){
				storedIndex = i;
				largestValue = BufferIndexes[i];
			}
			
		}
		return storedIndex;
		
	}
	
	/*Every transaction, increment all unpinned buffer values that
	were not used in the transaction.*/
	public void incrementIndex()
	{
		for (int i=0; i<BufferIndexes.length; i++)
		{
			if (BufferIndexes[i] > 0)
			{
				BufferIndexes[i] += 1;
			}
		}
	}
	
	/*Notes the index of a new pinned buffer*/
	public void newPin(Buffer buff)
	{
		int BuffIndex = 0;
		BufferIndexes[BuffIndex] = -1;
		incrementIndex();
	}

	/*Pins a buffer that was unpinned and makes note of
	 * that in index*/
	public void pin(Buffer buff)
	{
		int BuffIndex = Arrays.asList(bufferPool).indexOf(buff);
		BufferIndexes[BuffIndex] = -1;
		incrementIndex();
	}
	
	/*Unpins an existing pinned buffer and make it the
	 * most recently used buffer. */
	public void unpin(Buffer buff)
	{
		int BuffIndex = 0;
		incrementIndex();
		BufferIndexes[BuffIndex] = 1;
	}
	
	/*When a buffer is searched for and found, make it
	 * the most recently used buffer.*/
	public void find(int BuffIndex)
	{
		incrementIndex();
		BufferIndexes[BuffIndex] = 1;
	}

}
