package org.rugco.zk

import org.specs2.mutable._
import spray.json._
import scala.concurrent.ExecutionContext.Implicits.global
import spray.httpx.SprayJsonSupport

case class SimpleJson(str: String, int: Int)
case class OtherClasses(str: String, other: SimpleJson)
case class Existing(option: Option[OtherClasses], list: List[SimpleJson])
case class Recursive(me: Option[Recursive])
case class Typed[T](typed: T)

trait JsonMarshallers extends DefaultJsonProtocol with SprayJsonSupport with JsonMetaType {
  implicit val jfSimpleJson                           = jsonFormat2(SimpleJson)
  implicit val jfOtherClasses                         = jsonFormat2(OtherClasses)
  implicit val jfExisting                             = jsonFormat2(Existing)
  implicit val jfRecursive: RootJsonFormat[Recursive] = rootFormat(lazyFormat(jsonFormat(Recursive, "me")))
  implicit def jfTyped[T: JsonFormat]                 = jsonFormat1(Typed.apply[T])
}

class JsonSerializationTest extends SpecificationWithJUnit with ZkTest with JsonMarshallers {
  "(De-)serialize Scala Objects (with Spray JsonFormat support) to/from ZK" >> {
    val simpleJson = SimpleJson("one", 1)
    val other      = OtherClasses("two", SimpleJson("two", 2))
    val existing   = Existing(Some(other), List(simpleJson, simpleJson))
    val recursive  = Recursive(Some(Recursive(Some(Recursive(None)))))
    val typed1     = Typed(simpleJson)
    val typed2     = Typed(other)

    val res = for {
      _ <- ZkApi.delete("/test/zkapi", recursive = true)
    // simple
      x <- ZkApi.create("/test/zkapi/json/simple", simpleJson)
      y <- ZkApi.getData[SimpleJson](x.path)
    // other classes
      _ <- ZkApi.setData(x.path, other)
      z <- ZkApi.getData[OtherClasses](x.path)
    // existing scala API classes
      _ <- ZkApi.setData(x.path, existing)
      e <- ZkApi.getData[Existing](x.path)
    // recursive definitions
      _ <- ZkApi.setData(x.path, recursive)
      r <- ZkApi.getData[Recursive](x.path)
      // typed
      _ <- ZkApi.setData(x.path, typed1)
      p <- ZkApi.getData[Typed[SimpleJson]](x.path)
      _ <- ZkApi.setData(x.path, typed2)
      q <- ZkApi.getData[Typed[OtherClasses]](x.path)
      _ <- ZkApi.delete("/test/zkapi", recursive = true)
    } yield {
      x.value === simpleJson
      y.value === simpleJson
      z.value === other
      e.value === existing
      r.value === recursive
      p.value === typed1
      q.value === typed2
    }

    await(res)
    "" === "" // do nothing, await (delete operation) returns Future[Unit]
  }

  "(De-)serialize Plain JSON to/from ZK" >> {

  }
}
