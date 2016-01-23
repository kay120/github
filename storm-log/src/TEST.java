import org.junit.Test;



public class TEST {
	
	
	@Test
	public void incrementCount() {
	   int slot = 5;
	   long[] counts = new long[slot];
	   counts[slot - 1]++;
	   for(long c : counts){
		   System.out.println(c);
	   }
	   
	}
}
