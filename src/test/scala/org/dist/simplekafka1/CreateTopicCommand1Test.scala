package org.dist.simplekafka1

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class CreateTopicCommand1Test extends ZookeeperTestHarness {
  test("should assign partitions assigned to ") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient1 = new ZookeeperClient1(config)
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8080))

    val createCommandTest = new CreateTopicCommand1(zookeeperClient)
    val noOfPartitions = 3
    val replicationFactor = 2
    createCommandTest.createTopic("topic1", noOfPartitions, replicationFactor)
    val topics = zookeeperClient.getAllTopics()
    assert(topics.size == 1)

    val partitionAssignments = topics("topic1")
    assert(partitionAssignments.size == 2)
    partitionAssignments.foreach(p => assert(p.brokerIds.size == 3))
  }
}
