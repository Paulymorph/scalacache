package scalacache.redis_lettuce

import java.util.concurrent.CompletionStage

import io.lettuce.core.SetArgs
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.support.AsyncPool
import scalacache.logging.Logger
import scalacache.{AbstractCache, CacheConfig, Mode}

import scala.concurrent.duration.Duration

class RedisCache[V](pool: AsyncPool[StatefulRedisConnection[String, V]])(implicit val config: CacheConfig) extends AbstractCache[V] {
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

  override def close[F[_]]()(implicit mode: Mode[F]): F[Any] = mode.M.delay(pool.close())

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
