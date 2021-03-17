package com.utils.models

import java.sql.Timestamp

case class Event(userId: String,
                  eventId: String,
                  eventTime: Timestamp,
                  eventType: String,
                  attributes: Option[Map[String, String]]
                ) extends Ordered[Event] {

  override def compare(that: Event): Int = eventTime.compareTo(that.eventTime)

}