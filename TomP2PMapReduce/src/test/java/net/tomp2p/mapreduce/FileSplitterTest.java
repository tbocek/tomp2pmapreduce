package net.tomp2p.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import net.tomp2p.mapreduce.utils.FileSize;
import net.tomp2p.mapreduce.utils.FileSplitter;
import net.tomp2p.mapreduce.utils.FileUtils;
import net.tomp2p.peers.Number160;
 
public class FileSplitterTest {

	@Test
	public void test() {
		try {
			PeerMapReduce[] peer = TestExampleJob.createAndAttachNodes(1, 4253);

			TestExampleJob.bootstrap(peer);
			TestExampleJob.perfectRouting(peer);
			String filesPath = new File("").getAbsolutePath() + "/src/test/java/net/tomp2p/mapreduce/testfiles/";
			List<Number160> fileKeys = Collections.synchronizedList(new ArrayList<>());
			List<FutureMapReduceData> filePuts = Collections.synchronizedList(new ArrayList<>());

			List<String> pathVisitor = Collections.synchronizedList(new ArrayList<>());
			FileUtils.INSTANCE.getFiles(new File(filesPath), pathVisitor);
			assertEquals(7, pathVisitor.size());

			for (String filePath : pathVisitor) {
				Map<Number160, FutureMapReduceData> tmp = FileSplitter.splitWithWordsAndWrite(filePath, peer[0], 3, Number160.createHash("DOMAINKEY"), FileSize.MEGA_BYTE.value(), "UTF-8");
				assertEquals(1, tmp.keySet().size());
				fileKeys.addAll(tmp.keySet());
				filePuts.addAll(tmp.values());
			}
			assertEquals(pathVisitor.size(), fileKeys.size());
			assertEquals(pathVisitor.size(), filePuts.size());

			for (PeerMapReduce p : peer) {
				p.peer().shutdown().await();
			}
		} catch (Exception e) {

		}
	}

}
