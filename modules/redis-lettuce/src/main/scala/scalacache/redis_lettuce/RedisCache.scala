package scalacache.redis_lettuce

import java.util.concurrent.CompletionStage

import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.masterreplica.MasterReplica
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.support.{AsyncConnectionPoolSupport, AsyncPool, BoundedAsyncPool, BoundedPoolConfig}
import io.lettuce.core.{ClientOptions, ReadFrom, RedisClient, RedisURI, SetArgs}
import scalacache.logging.Logger
import scalacache.{AbstractCache, Cache, CacheConfig, Mode}

import scala.concurrent.duration.Duration

class RedisCache[V] private(
                             pool: AsyncPool[StatefulRedisConnection[String, V]],
                             resourcesCleanup: () => Unit
                           )(implicit val config: CacheConfig) extends AbstractCache[V] {
  override protected def doGet[F[_]](key: String)(implicit mode: Mode[F]): F[Option[V]] =
    withCommands { commands =>
      commands.get(key).thenApply { value =>
        val maybeValue = Option(value)
        logCacheHitOrMiss(key, maybeValue)
        maybeValue
      }
    }


  override protected def doPut[F[_]](key: String, value: V, ttl: Option[Duration])(implicit mode: Mode[F]): F[Any] =
    withCommands { commands =>
      logCachePut(key, ttl)
      ttl.filterNot(_ == Duration.Zero).fold(commands.set(key, value)) { expire =>
        val expiryArgs = SetArgs.Builder.px(expire.toMillis)
        commands.set(key, value, expiryArgs)
      }.asInstanceOf[CompletionStage[Any]]
    }

  override protected def doRemove[F[_]](key: String)(implicit mode: Mode[F]): F[Any] =
    withCommands { commands =>
      commands.del(key).asInstanceOf[CompletionStage[Any]]
    }

  override protected def doRemoveAll[F[_]]()(implicit mode: Mode[F]): F[Any] =
    withCommands { commands =>
      commands.flushdb().asInstanceOf[CompletionStage[Any]]
    }

  override protected def logger = Logger.getLogger(getClass.getName)

  override def close[F[_]]()(implicit mode: Mode[F]): F[Any] = mode.M.delay(resourcesCleanup(): Any)

  private def withCommands[F[_], T](f: RedisAsyncCommands[String, V] => CompletionStage[T])
                                   (implicit mode: Mode[F]): F[T] = mode.M.async { callback =>
    pool.acquire().thenCompose { connection =>
      val commands = connection.async()
      f(commands).thenCompose { result =>
        callback(Right(result))
        pool.release(connection)
      }
    }.exceptionally { error =>
      callback(Left(error))
      null
    }
  }
}

import scala.collection.JavaConverters._

object RedisCache {
  def apply[V](
                uri: RedisURI,
                clientResources: ClientResources,
                poolConfig: BoundedPoolConfig,
                clientOptions: ClientOptions = ClientOptions.create()
              )(implicit codec: RedisCodec[String, V], cacheConfig: CacheConfig): Cache[V] = {
    val client = RedisClient.create(clientResources)
    client.setOptions(clientOptions)
    val pool: BoundedAsyncPool[StatefulRedisConnection[String, V]] =
      AsyncConnectionPoolSupport.createBoundedObjectPool(
        () => client.connectAsync(codec, uri),
        poolConfig
      )
    new RedisCache(pool, () => {
      pool.close()
      client.shutdown()
    })
  }

  def masterReplica[V](redisUri: RedisURI, otherRedisUris: RedisURI*)
                      (
                        clientResources: ClientResources,
                        poolConfig: BoundedPoolConfig,
                        readFrom: ReadFrom = ReadFrom.MASTER_PREFERRED
                      )
                      (implicit codec: RedisCodec[String, V], cacheConfig: CacheConfig): Cache[V] = {
    val client = RedisClient.create(clientResources)

    val connectionFactory = if (otherRedisUris.isEmpty)
      () => MasterReplica.connectAsync(client, codec, redisUri)
    else {
      val urisJavaSeq = (redisUri +: otherRedisUris).asJava
      () => MasterReplica.connectAsync(client, codec, urisJavaSeq)
    }

    val pool: BoundedAsyncPool[StatefulRedisConnection[String, V]] =
      AsyncConnectionPoolSupport.createBoundedObjectPool(
        () => connectionFactory().thenApply { connection =>
          connection.setReadFrom(readFrom)
          connection
        },
        poolConfig
      )
    new RedisCache(pool, () => {
      pool.close()
      client.shutdown()
    })
  }
}
