package org.rugco.zk

import org.specs2.mutable._
import scala.concurrent.duration.{FiniteDuration, Duration}
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.actor.{Props, ActorSystem}
import akka.pattern.ask
import org.specs2.matcher.{Expectable, Matcher}
import org.apache.zookeeper.{CreateMode, WatchedEvent}
import org.apache.zookeeper.Watcher.Event.EventType

abstract trait ZkTest {
  def await[T](f: Future[T]): T = Await.result(f, Duration("5 seconds"))
}

class ZkApiTest extends SpecificationWithJUnit with ZkTest {
  import org.rugco.zk.PlainTextMetaType.ct

  "ZooKeeper API getData watchers" >> {
    var nodeUpdated = 0
    var nodeDeleted = 0
    val f: WatchedEvent => Boolean = e => e match {
      case _ if e.getType == EventType.NodeDataChanged => nodeUpdated = nodeUpdated + 1; true
      case _ if e.getType == EventType.NodeDeleted     => nodeDeleted = nodeDeleted + 1; false
      case _ => throw new IllegalStateException("Unknown ZK event type")
    }
    val res = for {
      _ <- ZkApi.delete("/test/zkapi", recursive = true)
      x <- ZkApi.create("/getData_Watcher")
      _ <- ZkApi.setData(x.path, "Data_1")
      _ <- ZkApi.getData[String](x.path, Some(f))
      _ <- ZkApi.setData(x.path, "Data_2")
      _ <- ZkApi.setData(x.path, "Data_3")
      _ <- ZkApi.setData(x.path, "Data_4")
      _ <- ZkApi.delete(x.path)
      _ <- ZkApi.delete("/test/zkapi", recursive = true)
    } yield x
    await (res)
    nodeUpdated === 3
    nodeDeleted === 1
  }

  "ZooKeeper API children watchers" >> {
    var nodeChildrenChanged = 0
    val f: WatchedEvent => Boolean = e => e match {
      case _ if e.getType == EventType.NodeChildrenChanged => nodeChildrenChanged = nodeChildrenChanged + 1; true
      case _ if e.getType == EventType.NodeDeleted => false
      case _ => throw new IllegalStateException(s"Unknown ZK event type: $e")
    }
    val res = for {
      _ <- ZkApi.delete("/test/zkapi", recursive = true)
      x <- ZkApi.create("/test/zkapi/children")
      _ <- ZkApi.children(x.path, Some(f))
      _ <- ZkApi.create("/test/zkapi/children/a")
      _ <- ZkApi.create("/test/zkapi/children/p",    createMode = CreateMode.PERSISTENT)
      _ <- ZkApi.create("/test/zkapi/children/e",    createMode = CreateMode.EPHEMERAL)
      _ <- ZkApi.create("/test/zkapi/children/pseq", createMode = CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- ZkApi.create("/test/zkapi/children/pseq", createMode = CreateMode.PERSISTENT_SEQUENTIAL)
      _ <- ZkApi.create("/test/zkapi/children/eseq", createMode = CreateMode.EPHEMERAL_SEQUENTIAL)
      _ <- ZkApi.create("/test/zkapi/children/eseq", createMode = CreateMode.EPHEMERAL_SEQUENTIAL)
      _ <- ZkApi.delete("/test/zkapi", recursive = true) // it creates only one event(!)
    } yield x
    await(res)
    nodeChildrenChanged === 8
  }
}

class ZkConnectionTest extends SpecificationWithJUnit {
  implicit val timeout = akka.util.Timeout(3000)

//  "Zk Connection should fail if no ZK instance is present" >> {
////    ZkConnection.execute(zk => {println(zk.getState.isConnected); println("OOO")})
//
//    //    val con = new ZkConnection
////    val result: Future[Unit] = for {
////      zk <- con.connect
////    } yield {
////      println("FINISHED")
////    }
//    "" === ""
//  }
}
