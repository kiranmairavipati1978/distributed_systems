package org.dist.simplekafka1

import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{BrokerChangeListener, ControllerExistsException, TopicChangeHandler}

class Controller1(val zookeeperClient: ZookeeperClient1, val brokerId: Int) {
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = -1

  def startup(): Unit = {
    zookeeperClient.subscribeControllerChangeListner(this)
    elect()
  }

  def addBroker(broker: Broker) = {
    liveBrokers += broker
  }

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      onBecomingLeader()
    } catch {
      case e: ControllerExistsException => {
        this.currentLeader = e.controllerId.toInt
      }
    }
  }

  def onBecomingLeader() = {
    liveBrokers = liveBrokers ++ zookeeperClient.getAllBrokers()
    zookeeperClient.subscribeBrokerChangeListener(new BrokerChangeListner1(this, zookeeperClient))
  }

  def setCurrent(existingControllerId: Int): Unit = {
    this.currentLeader = existingControllerId
  }
}
