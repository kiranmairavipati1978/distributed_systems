package org.dist.simplekafka1

import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.util.Networks

class ZookeeperClient1Test extends ZookeeperTestHarness{

  test("the broker should register") {
    // Given
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient1 = new ZookeeperClient1(config)

    // When
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8080))

    // Then
    assert(zookeeperClient.getAllBrokerIds().contains(0));
  }

  test("the controller should listen to subscribeBrokerChangeListener") {
    // Given
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val zookeeperClient: ZookeeperClient1 = new ZookeeperClient1(config)
    zookeeperClient.registerBroker(Broker(0, "10.10.10.10", 8080))
    val controller = new Controller1(zookeeperClient, config.brokerId)
    controller.startup()

    // when
    zookeeperClient.registerBroker(Broker(1, "10.10.10.11", 8001))

    // then
    TestUtils.waitUntilTrue(() => {
      controller.liveBrokers.size == 2
    }, "Waiting for all brokers to get added", 1000)

    assert(controller.liveBrokers.size == 2)
//    assert(zookeeperClient.getAllBrokerIds().contains(0));
  }

}
