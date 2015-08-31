package com.memsql.spark.interface.api

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Actor}
import akka.util.Timeout
import com.memsql.spark.interface.util.{Clock, BaseException}
import scala.concurrent.duration._
import com.memsql.spark.etl.api.configs._
import spray.json._

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

import PipelineState._

case class ApiException(message: String) extends BaseException(message: String)

object ApiActor {
  case object Ping
  case object PipelineQuery
  case class PipelineGet(pipeline_id: String)
  case class PipelinePut(pipeline_id: String, batch_interval: Long, config: PipelineConfig)
  case class PipelineUpdate(pipeline_id: String, state: Option[PipelineState] = None,
                            batch_interval: Option[Long] = None,
                            config: Option[PipelineConfig] = None,
                            trace_batch_count: Option[Int] = None,
                            error: Option[String] = None, _validate: Boolean = false)
  case class PipelineMetrics(pipeline_id: String, last_timestamp: Option[Long])
  case class PipelineTraceBatchDecrement(pipeline_id: String)
  case class PipelineDelete(pipeline_id: String)
  implicit val timeout = Timeout(5.seconds)
}

class ApiActor extends Actor with ApiService {
  override def receive = handleMessage
}

trait ApiService {
  import ApiActor._
  def sender: ActorRef

  private var pipelines = Map[String, Pipeline]()

  private[interface] def clock = new Clock()

  def handleMessage: Receive = {
    case Ping => sender ! "pong"
    case PipelineQuery => sender ! pipelines.values.toList
    case PipelineGet(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => sender ! Success(pipeline)
        case None => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelinePut(pipeline_id, batch_interval, config) => {
      try {
        pipelines.get(pipeline_id) match {
          case p: Some[Pipeline] => sender ! Failure(ApiException(s"pipeline with id $pipeline_id already exists"))
          case _ => {
            pipelines = pipelines + (pipeline_id -> Pipeline(pipeline_id, RUNNING, batch_interval, config, clock.currentTimeMillis))
            sender ! Success(true)
          }
        }
      } catch {
        case e: ApiException => sender ! Failure(e)
        case NonFatal(e) => sender ! Failure(ApiException(s"unexpected exception: $e"))
      }
    }
    case PipelineUpdate(pipeline_id, state, batch_interval, config, trace_batch_count, error, _validate) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          var updated = false
          var newState = pipeline.state
          var newBatchInterval = pipeline.batch_interval
          var newConfig = pipeline.config
          var newError = pipeline.error
          var newTraceBatchCount = pipeline.traceBatchCount

          try {
            if (state.isDefined) {
              newState = (pipeline.state, state.get, _validate) match {
                case (_, _, false) => state.get
                case (RUNNING, STOPPED, _) => state.get
                case (STOPPED | ERROR, RUNNING, _) => state.get
                case (prev, next, _) if prev == next => state.get
                case (prev, next, _) => throw new ApiException(s"cannot update state from $prev to $next")
              }

              updated |= newState != pipeline.state
            }

            if (batch_interval.isDefined) {
              newBatchInterval = batch_interval.get
              updated |= newBatchInterval != pipeline.batch_interval
            }

            if (config.isDefined) {
              newConfig = config.get
              // providing the config always counts as an update since the jar at the url may have changed
              updated = true
            }

            if (error.isDefined) {
              newError = error
              updated |= newError != pipeline.error
            }

            if (trace_batch_count.isDefined) {
              newTraceBatchCount = trace_batch_count.get
              updated |= newTraceBatchCount != pipeline.traceBatchCount
            }

            // update all fields in the pipeline and respond with success
            if (updated) {
              val newLastUpdated = clock.currentTimeMillis
              val newPipeline = Pipeline(pipeline_id, state=newState, batch_interval=newBatchInterval, last_updated=newLastUpdated, config=newConfig, error=newError)
              newPipeline.traceBatchCount = newTraceBatchCount
              newPipeline.metricsQueue = pipeline.metricsQueue
              pipelines = pipelines + (pipeline_id -> newPipeline)
            }
            sender ! Success(updated)
          } catch {
            case e: ApiException => sender ! Failure(e)
            case NonFatal(e) => sender ! Failure(ApiException(s"unexpected exception: $e"))
          }
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineTraceBatchDecrement(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          var updated = false
          var newTraceBatchCount = pipeline.traceBatchCount
          if (pipeline.traceBatchCount > 0) {
            pipeline.traceBatchCount = pipeline.traceBatchCount - 1
            updated = true
          }
          sender ! Success(updated)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineMetrics(pipeline_id, last_timestamp) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          val lastTimestamp = last_timestamp match {
            case Some(t) => t
            case None => 0
          }
          val metricRecords = pipeline.metricsQueue.filter(_.timestamp > lastTimestamp).toList
          sender ! Success(metricRecords)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case PipelineDelete(pipeline_id) => {
      pipelines.get(pipeline_id) match {
        case Some(pipeline) => {
          pipelines = pipelines -- Set(pipeline_id)
          sender ! Success(true)
        }
        case _ => sender ! Failure(ApiException(s"no pipeline exists with id $pipeline_id"))
      }
    }
    case default => sender ! ApiException("invalid api message type")
  }
}