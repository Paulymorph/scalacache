package scalacache.redis_letucce

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import io.lettuce.core.codec.{RedisCodec, StringCodec}
import io.lettuce.core.resource.{ClientResources, DefaultClientResources}
import io.lettuce.core.support.BoundedPoolConfig
import io.lettuce.core.{RedisClient, RedisURI}
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{Assertion, BeforeAndAfterAll, FlatSpec, Matchers}
import org.testcontainers.containers.wait.strategy.Wait
import scalacache.Cache
import scalacache.modes.scalaFuture._
import scalacache.redis_lettuce.RedisCache
import scalacache.serialization.Codec
import scalacache.serialization.binary._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

case class CaseClass(a: Int, b: String) extends Serializable

class RedisCacheSpec extends FlatSpec with Matchers with ScalaFutures with Eventually with BeforeAndAfterAll
  with ForAllTestContainer with IntegrationPatience with RedisCacheUtils {
  override val container =
    GenericContainer(
      "redis:alpine",
      exposedPorts = List(6379),
      waitStrategy = Wait.forListeningPort()
    )

  override def afterAll(): Unit = {
    client.shutdown()
    super.afterAll()
  }

  container.start()
  implicit val redisUri = RedisURI.create(s"redis://${container.containerIpAddress}:${container.mappedPort(6379)}/0")
  implicit val stringCodec: RedisCodec[String, String] = StringCodec.UTF8
  implicit def scalacacheCodecToRedis[T](implicit codec: Codec[T]): RedisCodec[String, T] = new RedisCodec[String, T] {
    override def decodeKey(bytes: ByteBuffer): String = StringCodec.UTF8.decodeKey(bytes)
    override def decodeValue(bytes: ByteBuffer): T = {
      val byteArray = Array.ofDim[Byte](bytes.remaining())
      bytes.get(byteArray)
      codec.decode(byteArray).getOrElse(???)
    }
    override def encodeKey(key: String): ByteBuffer = StringCodec.UTF8.encodeKey(key)
    override def encodeValue(value: T): ByteBuffer = ByteBuffer.wrap(codec.encode(value))
  }

  implicit val clientResources: ClientResources = ClientResources.builder()
      .ioThreadPoolSize(DefaultClientResources.MIN_IO_THREADS)
      .computationThreadPoolSize(DefaultClientResources.MIN_COMPUTATION_THREADS)
      .build()
  val client = RedisClient.create(redisUri)

  behavior of "get"
  it should "return None if key is absent" in withCache[String, Assertion] { cache =>
    whenReady(cache.get("key")) {
      _ shouldBe empty
    }
  }

  it should "return key if it was put" in withCache[String, Assertion] { cache =>
    val key = "put and retrieve"
    val value = "value to retrieve"
    client.connect().sync().set(key, value)
    whenReady(cache.get(key)) {
      _ should contain(value)
    }
  }

  behavior of "put"
  it should "put a key and value" in withCache[String, Assertion] { cache =>
    val key = "put"
    val value = "put value"
    whenReady(cache.put(key)(value)) { _ =>
      client.connect().sync().get(key) shouldBe value
    }
  }

  it should "delete key after TTL expiration" in withCache[String, Assertion] { cache =>
    val ttl = 5.seconds
    val key = "put"
    whenReady(cache.put(key)("value", Some(ttl))) {
      _ => succeed
    }

    eventually {
      client.connect().sync().get(key) shouldBe null
    }
  }

  it should "put with zero ttl" in withCache[String, Assertion] { cache =>
    val key = "put"
    val value = "value"
    whenReady(cache.put(key)(value, Some(Duration.Zero))) { _ =>
      client.connect().sync().get(key) shouldBe value
      client.connect().sync().ttl(key) shouldBe -1
    }
  }

  it should "put with TTL of less than 1 second and expire" in withCache[String, Assertion] { cache =>
    val key = "key"
    val cacheValue = "value"
    whenReady(cache.put(key)(cacheValue, Some(800.milliseconds))) { _ =>
      client.connect().sync().get(key) shouldBe cacheValue
      client.connect().sync().pttl(key).toLong should be > 0L

      eventually {
        client.connect().sync().get(key) shouldBe null
      }
    }
  }

  behavior of "serialization"

  def roundTrip[V](cache: Cache[V],  key: String, value: V)(implicit codec: RedisCodec[String, V]): Future[Option[V]] =
      for {
        _ <- cache.put(key)(value)
        result <- cache.get(key)
      } yield result

  it should "round-trip a String" in withCache[String, Assertion] { cache =>
    whenReady(roundTrip(cache, "string", "hello")) { _ should be(Some("hello")) }
  }

  it should "round-trip a byte array" in withCache[Array[Byte], Assertion] { cache =>
    whenReady(roundTrip(cache, "bytearray", "world".getBytes("utf-8"))) { result =>
      new String(result.get, "UTF-8") should be("world")
    }
  }

  it should "round-trip an Int" in withCache[Int, Assertion] { cache =>
    whenReady(roundTrip(cache, "int", 345)) { _ should be(Some(345)) }
  }

  it should "round-trip a Double" in withCache[Double, Assertion] { cache =>
    whenReady(roundTrip(cache, "double", 1.23)) { _ should be(Some(1.23)) }
  }

  it should "round-trip a Long" in withCache[Long, Assertion] { cache =>
    whenReady(roundTrip(cache, "long", 3456L)) { _ should be(Some(3456L)) }
  }

  it should "round-trip a Serializable case class" in withCache[CaseClass, Assertion] { cache =>
    val cc = CaseClass(123, "wow")
    whenReady(roundTrip(cache, "caseclass", cc)) { _ should be(Some(cc)) }
  }


  behavior of "caching"
  it should "cache computation" in withCache[Int, Assertion] { cache =>
    val computationCount = new AtomicInteger(0)
    val key = "key"

    def compute = cache.caching(key)(None)(computationCount.incrementAndGet())
    whenReady(for {
      _ <- compute
      result <- compute
    } yield result) {
      _ shouldBe 1
    }
    computationCount.get() shouldBe 1
  }
}

trait RedisCacheUtils {
  def withCache[V, T](f: Cache[V] => T)(implicit codec: RedisCodec[String, V], uri: RedisURI, clientResources: ClientResources): T = {
    withClient { client =>
      client.connect(uri).sync().flushdb()
    }
    val boundedPoolConfig = BoundedPoolConfig.builder()
      .maxTotal(1)
      .build()
    val cache = RedisCache[V](uri, clientResources, boundedPoolConfig)
    f(cache)
  }

  def withClient[T](f: RedisClient => T)(implicit uri: RedisURI, clientResources: ClientResources): T = {
    val client = RedisClient.create(clientResources, uri)
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }
}
