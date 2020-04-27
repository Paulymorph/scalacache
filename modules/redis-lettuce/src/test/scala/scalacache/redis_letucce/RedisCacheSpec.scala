package scalacache.redis_letucce

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

import com.dimafeng.testcontainers.{ForAllTestContainer, GenericContainer}
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.{RedisCodec, StringCodec}
import io.lettuce.core.support.{AsyncConnectionPoolSupport, AsyncPool, BoundedAsyncPool, BoundedPoolConfig}
import io.lettuce.core.{RedisClient, RedisURI}
import org.scalatest.concurrent.{Eventually, IntegrationPatience, ScalaFutures}
import org.scalatest.{Assertion, BeforeAndAfterAll, FlatSpec, Matchers}
import org.testcontainers.containers.wait.strategy.Wait
import scalacache.Cache
import scalacache.modes.scalaFuture._
import scalacache.redis_lettuce.RedisCache
import scalacache.serialization.Codec
import scalacache.serialization.binary.{IntBinaryCodec, StringBinaryCodec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

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
  implicit def scalacacheCodecToRedis[T](implicit codec: Codec[T]): RedisCodec[String, T] = new RedisCodec[String, T] {
    override def decodeKey(bytes: ByteBuffer): String = StringCodec.UTF8.decodeKey(bytes)
    override def decodeValue(bytes: ByteBuffer): T =
      if (bytes.isReadOnly) ???
      else if (bytes.hasArray)
        codec.decode(bytes.array()).getOrElse(???)
      else null.asInstanceOf[T]
    override def encodeKey(key: String): ByteBuffer = StringCodec.UTF8.encodeKey(key)
    override def encodeValue(value: T): ByteBuffer = ByteBuffer.wrap(codec.encode(value))
  }

  implicit val client = RedisClient.create(redisUri)

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
  def withCache[V, T](f: Cache[V] => T)(implicit client: RedisClient, codec: RedisCodec[String, V], uri: RedisURI): T = {
    withAsyncPool[V, T] { pool =>
      client.connect(uri).sync().flushdb()
      val cache = new RedisCache[V](pool)
      f(cache)
    }
  }

  def withClient[T](f: RedisClient => T)(implicit uri: RedisURI): T = {
    val client = RedisClient.create(uri)
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

  private def withAsyncPool[V, T](f: AsyncPool[StatefulRedisConnection[String, V]] => T)
                         (implicit client: RedisClient, codec: RedisCodec[String, V], uri: RedisURI): T = {
    val pool: BoundedAsyncPool[StatefulRedisConnection[String, V]] =
      AsyncConnectionPoolSupport.createBoundedObjectPool(
        () => client.connectAsync(codec, uri),
        BoundedPoolConfig.create()
      )
    try {
      f(pool)
    } finally {
      pool.close()
    }
  }
}
