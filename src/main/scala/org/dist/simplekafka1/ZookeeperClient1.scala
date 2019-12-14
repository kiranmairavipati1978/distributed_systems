package org.dist.simplekafka1

import com.google.common.annotations.VisibleForTesting
import org.I0Itec.zkclient.{IZkChildListener, IZkDataListener, ZkClient}
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.server.Config
import org.dist.queue.utils.ZKStringSerializer
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{Controller, ControllerExistsException}

import scala.jdk.CollectionConverters._

class ZookeeperClient1(config: Config) {
  val BrokerTopicsPath = "/brokers/topics"
  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"

  private val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)

  @VisibleForTesting
  def registerBroker(broker: Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }

  private def getBrokerPath(id:Int) = {
    BrokerIdsPath + "/" + id
  }

  def createEphemeralPath(client: ZkClient, path: String, data: String): Unit = {
    try {
      client.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(client, path)
        client.createEphemeral(path, data)
      }
    }
  }

  private def createParentPath(client: ZkClient, path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      client.createPersistent(parentDir, true)
  }

  def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data:String = zkClient.readData(getBrokerPath(brokerId))
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): Option[List[String]] = {
    val result = zkClient.subscribeChildChanges(BrokerIdsPath, listener)
    Option(result).map(_.asScala.toList)
  }

  def subscribeControllerChangeListner(controller:Controller1): Unit = {
    zkClient.subscribeDataChanges(ControllerPath, new ControllerChangeListner1(controller))
  }

  def tryCreatingControllerPath(controllerId: String): Unit = {
    try {
      createEphemeralPath(zkClient, ControllerPath, controllerId)
    } catch {
      case e:ZkNodeExistsException => {
        val existingControllerId:String = zkClient.readData(ControllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(brokerId => {
      val data:String = zkClient.readData(getBrokerPath(brokerId.toInt))
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  class ControllerChangeListner1(controller:Controller1) extends IZkDataListener {
    override def handleDataChange(dataPath: String, data: Any): Unit = {
      val existingControllerId:String = zkClient.readData(dataPath)
      controller.setCurrent(existingControllerId.toInt)
    }

    override def handleDataDeleted(dataPath: String): Unit = {
      controller.elect()
    }
  }
}
