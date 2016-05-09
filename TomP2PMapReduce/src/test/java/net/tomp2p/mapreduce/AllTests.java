package net.tomp2p.mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({  FileSplitterTest.class, SerializeUtilsTest.class, TaskRPCTest.class, TestDistributedTask.class, TestMapReduceValue.class, TestExampleJob.class })
public class AllTests {

}
