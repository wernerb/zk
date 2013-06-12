package org.rugco.zk

import grizzled.slf4j.Logging
import spray.httpx.marshalling._
import org.apache.zookeeper._
import scala.concurrent._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.AsyncCallback._
import spray.httpx.unmarshalling._
import org.apache.zookeeper.data.Stat
import spray.http.{ContentType, HttpBody}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConversions._
import scala.Some


// wrapper around znode
// may want to have implicits from ZkNode -> future, and, possibly, from ZkNode[T] to T (check if it creates any issues)
case class ZkNode[A] (path: String, value: A, version: Int = -1) {
  def map[B](f: A => B): ZkNode[B] = ZkNode(path, f(value), version)
  def flatMap[B](f: A => ZkNode[B]): ZkNode[B] = f(value)

  def name = path.split('/').last
  def dir  = path.split('/').init.mkString("/")

  def toFuture = future(value)
}


// the following code is needed to tell the API what content type is used during the encoding
// you need both the content type and its marshaller/un-marshaller to be available as implicits
// by default, nothing happens
trait MetaType[T] {
  def contentType: ContentType
}

trait PlainTextMetaType {
  implicit def ct[T] = new MetaType[T] {
    def contentType = ContentType.`text/plain`
  }
}

trait JsonMetaType {
  implicit def ct[T] = new MetaType[T] {
    def contentType = ContentType.`application/json`
  }
}

object PlainTextMetaType extends PlainTextMetaType


object ZkApi extends Logging {
  val RC_NO_PARENT   = -110
  val RC_NODE_EXISTS = -101

  private[zk] def promised[T](f: Promise[T] => Unit): Future[T] = {
    val p = promise[T]; f(p); p.future
  }

  private[zk] def watcher(f: Option[WatchedEvent => Boolean], resetWatcher: => Unit) = f match {
    case Some(func) => new Watcher() {
      def process(event: WatchedEvent) {
        if (func(event)) resetWatcher
      }
    }
    case _ => null
  }

  private[zk] def errorResult[T](operation: String, p: Promise[T], rc: Int, path: String) = {
    val ex = KeeperException.create(KeeperException.Code.get(rc), path)
    warn(s"ZK: $operation an exception occurred: $ex")
    p.failure(ex)
  }

  /**
   *
   * @param forceIfPossible creates nodes recursively, even if they do not have parents; parents are always created PERSISTENT
   */
  def create[T: Marshaller](path: String, data: T = "", createMode: CreateMode = CreateMode.PERSISTENT, forceIfPossible: Boolean = true): Future[ZkNode[T]] = promised[ZkNode[T]] { p =>
    ZkConnection.execute { zk =>
      zk.create(path, marshalUnsafe(data).buffer, Ids.OPEN_ACL_UNSAFE, createMode,
        new StringCallback {
          def processResult(rc: Int, path: String, ctx: Any, name: String) = rc match {
            case 0 =>
              debug(s"ZK: $path created, $createMode $data")
              p.success(ZkNode(path, data))
            case _ if rc == KeeperException.Code.NONODE.intValue && forceIfPossible =>
              p.completeWith {
                for {
                  _ <- create(ZkNode(path, data).dir)
                  x <- create(path, data, createMode, forceIfPossible)
                } yield x
              }
            case _ if rc == KeeperException.Code.NODEEXISTS.intValue && forceIfPossible =>
              warn(s"ZK: node for $path already exists, create operation does nothing and still successful")
              p.success(ZkNode(path, data))
            case _ => errorResult("create", p, rc, path)
          }
        }, None)
    }
  }

  /**
   * @param recursive deletes path, even if it has children. Children are deleted with version = -1
   * @return successful, even if path does not exists (Java Zk throws NoNode KeeperException)
   */
  def delete(path: String, version: Int = -1, recursive: Boolean = false): Future[Unit] = promised[Unit] { p =>
    ZkConnection.execute { zk =>
      zk.delete(path, version, new VoidCallback {
        def processResult(rc: Int, path: String, ctx: Any) = rc match {
          case _ if rc == 0 =>
            debug(s"ZK: $path deleted, recursive = $recursive")
            p.success {}
          case _ if (rc == KeeperException.Code.NOTEMPTY.intValue && recursive) =>
            p.completeWith {
              for {
                children <- children(path)
                _        <- Future.reduce(children.map(child => delete(path + (if (path.endsWith("/")) "" else "/") + child, recursive = true)))((x, y) => x)
              } yield delete(path, version, true)
            }
          case _ if (rc == KeeperException.Code.NONODE.intValue) =>
            warn(s"ZK: no node for $path, delete operation still successful")
            p.success {}
          case _ => errorResult("delete", p, rc, path)
        }
      }, None)
    }
  }

  def getData[T: Unmarshaller : MetaType](path: String, f: Option[WatchedEvent => Boolean] = None): Future[ZkNode[T]] = promised[ZkNode[T]] { p =>
    ZkConnection.execute { zk =>
      zk.getData(path, watcher(f, getData(path, f)), new DataCallback {
        def processResult(rc: Int, path: String, ctx: Any, data: Array[Byte], stat: Stat) = rc match {
          case 0 => p.success(ZkNode(path, unmarshalUnsafe(HttpBody(implicitly[MetaType[T]].contentType, if (data == null) Array.empty[Byte] else data)), stat.getVersion))
          case _ => errorResult("getData", p, rc, path)
        }
      }, None)
    }
  }

  def setData[T: Marshaller](path: String, data: T, version: Int = -1): Future[ZkNode[T]] = promised[ZkNode[T]] { p =>
    ZkConnection.execute { zk =>
      zk.setData(path, marshalUnsafe(data).buffer, version, new StatCallback {
        def processResult(rc: Int, path: String, ctx: Any, stat: Stat) = rc match {
          case 0 =>
            debug(s"ZK: $path updated $data")
            p.success(ZkNode(path, data, stat.getVersion))
          case _ => errorResult("setData", p, rc, path)
        }
      }, None)
    }
  }

  def children(path: String, f: Option[WatchedEvent => Boolean] = None): Future[List[String]] = promised[List[String]] { p =>
    ZkConnection.execute { zk =>
      zk.getChildren(path, watcher(f, children(path, f)), new Children2Callback {
        def processResult(rc: Int, path: String, ctx: Any, children: java.util.List[String], stat: Stat) =
          if (rc == 0) p.success(children.toList) else errorResult("children", p, rc, path)

      }, None)
    }
  }
}
