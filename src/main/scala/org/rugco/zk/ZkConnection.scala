package org.rugco.zk

import scala.concurrent._
import scala.concurrent.duration._
import org.apache.zookeeper.{Watcher, WatchedEvent, ZooKeeper}
import akka.actor.{Props, ActorSystem, Actor}
import akka.pattern.ask
import akka.pattern.pipe
import org.rugco.server.Settings
import org.apache.zookeeper.Watcher.Event.KeeperState
import grizzled.slf4j.Logging
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.reflect.ClassTag


private[zk] object ZkConnection {
  implicit val timeout = akka.util.Timeout(Settings.timeout)

  case object Zk
  case object Connect
  case class Execute[T](f: ZooKeeper => T)

  val connection = ActorSystem("zkconnection").actorOf(Props(classOf[ZkConnection]))
  connection ! ZkConnection.Connect

  def execute[T : ClassTag](f: ZooKeeper => T): Future[T] = (connection ? Execute(f)).mapTo[T]
}

private[zk] class ZkConnection extends Actor with Watcher with Logging {
  private var connectionPromise  = promise[ZooKeeper]
  private var zk: ZooKeeper      = null // TODO: avoid using null!

  def connect: Future[ZooKeeper] = {
    connectionPromise = promise[ZooKeeper]
    zk = new ZooKeeper(Settings.zkServers.mkString(","), 5000, this)  // avoid hardcoding
    connectionPromise.future
  }

  override def process(event: WatchedEvent) {
    info("ZK event received: " + event)
    if (event.getState == KeeperState.SyncConnected) {
      info("Connection with Zookeeper has been successfully established")
      connectionPromise.success { zk }
    }
    if (event.getState == KeeperState.Expired) {
      warn("Current session expired with ZK, this ZK object is no longer valid!") // do something fancier than just reconnect every time
      self ! ZkConnection.Connect
    }
  }

  def receive = {
    case ZkConnection.Connect    => sender ! (if (zk != null && zk.getState.isConnected) zk else Await.result(connect, Duration.Inf))
    case ZkConnection.Execute(f) => future { f(zk) } pipeTo sender // leave the processing fast

    case m => throw new IllegalArgumentException(s"Unknown message $m")
  }
}