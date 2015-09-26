package simpledb.buffer;

import java.util.Arrays;

/*CS4432-Project1
 * This file contains all of code in order to execute
 * the clock replacement policy.*/

public class ClockReplacement implements ReplacementPolicy {
	
	/* CS4432-Project1 List of the status of the buffer. The value stored at an
	index represents whether the data is pinned, unpinned, 
	or was used recently. 0 means there's no data, -1 means it
	is pinned, 1 means that the data could be sent back to the 
	disk, 2 means the data will not be sent back to disk.*/
	public int[] BufferIndexes;
	
	/* CS4432-Project1 The index which the clock hand is currently on.*/
	public int clockHand = 0;
	
	private Buffer[] bufferPool;
	
	
	ClockReplacement(Buffer[] bufferPool){
		this.bufferPool = bufferPool;
	}
	
	/*CS4432-Project1 Creates a clean list of 0s.*/
	public void fillArray(int NumBuffers)
	{
		for (int i=0; i<NumBuffers; i++) 
			{
				BufferIndexes[i] = 0;
			}
	}
	
	/*CS4432-Project1 Uses clock replacement policy to replace a buffer. Only
	 * to be called if the buffer is full.*/
	public Buffer indexToReplace()
	{
		int clockHandLastChange = clockHand;
		int replacementIndex = -1;
		
		/*CS4432-Project1 Checks if current clock hand location is unpinned
		 * and was checked before (1 value). If it is, return
		 * that index. If not, set it to 1, and move to next
		 * buffer.*/
		if (BufferIndexes[clockHand] != -1)
		{
			if (BufferIndexes[clockHand] != 1)
			{
				replacementIndex = clockHand;
			}
			else 
			{
				BufferIndexes[clockHand] = 1;
				NextBuffer();
			}
		}
		
		/* CS4432-Project1 Looks through the rest of the buffers to see if any
		 * is unpinned and had been checked before (1 value).
		 * If one of them is, return that index. If not, set
		 * the value of that index to 1 and move to the next
		 * buffer until we the last index value we swapped from a 2 to a 1.*/
		else 
		{
			NextBuffer();
			while (clockHand != clockHandLastChange)
			{
				if (BufferIndexes[clockHand] != -1)
				{
					if (BufferIndexes[clockHand] == 1)
					{
						replacementIndex = clockHand;
					}
					else 
					{
						clockHandLastChange = clockHand;
						BufferIndexes[clockHand] = 1;
						NextBuffer();
					}
				}
				else 
				{
					NextBuffer();
				}
			}
		}
		
		/* CS4432-Project1 This means that we at the location where a buffer with
		 * a 2 value was changed to a 1. As long as this buffer is unpinned, it 
		 * can be replaced.*/
		if (BufferIndexes[clockHand] != -1)
		{
			replacementIndex = clockHand;
		}
		if (replacementIndex == -1) {
			return null;
		}
		return bufferPool[replacementIndex];
	}
	
	/* CS4432-Project1 Rotates the clock hand to the next buffer. If we are at
	 * the final buffer, we start from the beginning of the
	 * index list.*/
	private void NextBuffer()
	{
		clockHand++;
		if (clockHand > BufferIndexes.length-1)
		{
			clockHand = 0;
		}
	}
	
	/*CS4432-Project1 Adds a new pinned buffer and moves the clock hand*/
	public void newPin(Buffer buff)
	{
		int BuffIndex = Arrays.asList(bufferPool).indexOf(buff);
		BufferIndexes[BuffIndex] = -1;
		NextBuffer();
	}

	/*CS4432-Project1 Pins an existing buffer that was unpinned*/
	public void pin(Buffer buff)
	{
		int BuffIndex = Arrays.asList(bufferPool).indexOf(buff);
		BufferIndexes[BuffIndex] = -1;
	}
	
	/*CS4432-Project1 Unpins an existing buffer that was pinned*/
	public void unpin(Buffer buff)
	{
		int BuffIndex = Arrays.asList(bufferPool).indexOf(buff);
		BufferIndexes[BuffIndex] = 2;
	}

}
