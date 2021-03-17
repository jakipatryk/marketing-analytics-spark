package com.utils

import com.utils
import com.utils.models.{Event, PurchaseWithSessionId}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

import scala.annotation.tailrec
import scala.collection.SortedSet

case class SessionBuffer(appOpenEvents: SortedSet[Event], purchaseEvents: SortedSet[Event])

object PurchasesWithSessionAggregator
  extends Aggregator[Event, SessionBuffer, List[PurchaseWithSessionId]] with Serializable {

  override def zero: SessionBuffer = utils.SessionBuffer(SortedSet.empty, SortedSet.empty)

  override def reduce(b: SessionBuffer, a: Event): SessionBuffer = a.eventType match {
    case "app_open" => SessionBuffer(b.appOpenEvents + a, b.purchaseEvents)
    case "purchase" => SessionBuffer(b.appOpenEvents, b.purchaseEvents + a)
    case _ => b
  }

  override def merge(b1: SessionBuffer, b2: SessionBuffer): SessionBuffer =
    SessionBuffer(b1.appOpenEvents ++ b2.appOpenEvents, b1.purchaseEvents ++ b2.purchaseEvents)

  override def finish(reduction: SessionBuffer): List[PurchaseWithSessionId] = {
    @tailrec
    def getFirstStartingBefore(appOpenEvents: SortedSet[Event], purchase: Event): (Event, SortedSet[Event]) = {
      val fst = appOpenEvents.head
      if(fst <= purchase) (fst, appOpenEvents)
      else getFirstStartingBefore(appOpenEvents.tail, purchase)
    }

    val (result, _) = reduction.purchaseEvents.foldLeft((List[PurchaseWithSessionId](), reduction.appOpenEvents)) {
      (acc, purchase) => {
        val (appOpenEvent, appOpenEvents) = getFirstStartingBefore(acc._2, purchase)
        val campaignId = appOpenEvent.attributes.map(_("campaign_id")).getOrElse("")
        val channelIid = appOpenEvent.attributes.map(_("channel_id")).getOrElse("")
        val purchasesWithCurrent = purchase
          .attributes
          .map(m => PurchaseWithSessionId(m("purchase_id"), appOpenEvent.eventId, campaignId, channelIid) :: acc._1)
          .getOrElse(acc._1)
        (purchasesWithCurrent, appOpenEvents)
      }
    }

    result
  }

  override def outputEncoder: Encoder[List[PurchaseWithSessionId]] = Encoders.product[List[PurchaseWithSessionId]]

  override def bufferEncoder: Encoder[SessionBuffer] = Encoders.kryo[SessionBuffer]

}
