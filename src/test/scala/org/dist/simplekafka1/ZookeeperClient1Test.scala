package org.dist.simplekafka1

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class ZookeeperClient1Test extends ZookeeperTestHarness{

  test("the broker should register") {

    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient1 = new ZookeeperClient1(config)

    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8080))

    assert(zookeeperClient.getAllBrokerIds().contains(0));

  }

}
