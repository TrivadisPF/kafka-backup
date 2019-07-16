import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import ch.tbd.kafka.backuprestore.PartitionSelector;

public class TestPartitionSelector {

	@Test
	public void testAllEquals() {
		PartitionSelector ps = new PartitionSelector(4);
		assertEquals(0, ps.nextPartition());
		assertEquals(1, ps.nextPartition());
		assertEquals(2, ps.nextPartition());
		assertEquals(3, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
	}

	@Test
	public void testFirstPartitionWith4() {
		List<Integer> repeatPartition = new ArrayList<>(Arrays.asList(4,1,1,1));
		PartitionSelector ps = new PartitionSelector(4, repeatPartition);
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(1, ps.nextPartition());
		assertEquals(2, ps.nextPartition());
		assertEquals(3, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(1, ps.nextPartition());
		assertEquals(2, ps.nextPartition());
		assertEquals(3, ps.nextPartition());
	}

	@Test
	public void testFirstPartitionWith4andDefault() {
		List<Integer> repeatPartition = new ArrayList<>(Arrays.asList(4));
		PartitionSelector ps = new PartitionSelector(4, repeatPartition);
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(1, ps.nextPartition());
		assertEquals(2, ps.nextPartition());
		assertEquals(3, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(0, ps.nextPartition());
		assertEquals(1, ps.nextPartition());
		assertEquals(2, ps.nextPartition());
		assertEquals(3, ps.nextPartition());
	}

	
}
