package org.dist.simplekafka1

import org.dist.queue.utils.ZkUtils.Broker

class Controller1(val zookeeperClient: ZookeeperClient1, val brokerId: Int) {
  var liveBrokers: Set[Broker] = Set()

  def startup(): Unit = {
    zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListner1(this, zookeeperClient))
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }
}
