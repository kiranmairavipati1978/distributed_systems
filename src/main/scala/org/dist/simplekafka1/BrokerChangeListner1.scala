package org.dist.simplekafka1

import java.util

import org.I0Itec.zkclient.IZkChildListener

class BrokerChangeListner1(controller:Controller1, zookeeperClient:ZookeeperClient1) extends IZkChildListener {
  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentBrokerList: util.List[String]): Unit = {
    try {
      val curBrokerIds = currentBrokerList.asScala.map(_.toInt).toSet
      val newBrokerIds = curBrokerIds -- controller.liveBrokers.map(broker  => broker.id)
      val newBrokers = newBrokerIds.map(zookeeperClient.getBrokerInfo(_))

      newBrokers.foreach(controller.addBroker(_))

//      if (newBrokerIds.size > 0)
//        controller.onBrokerStartup(newBrokerIds.toSeq)

    }
  }
}
