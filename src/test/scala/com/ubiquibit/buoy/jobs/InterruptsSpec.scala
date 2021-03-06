/*
 * Copyright (c) 2019.
 *
 * This work, (c) by jason@ubiquibit.com
 *
 * This work is licensed under a
 * Creative Commons Attribution-ShareAlike 4.0 International License.
 *
 * You should have received a copy of the license along with this
 * work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
 *
 */

package com.ubiquibit.buoy.jobs

import com.ubiquibit.RandomData
import com.ubiquibit.buoy.TextRecord
import com.ubiquibit.buoy.jobs.StationInterrupts.sortRecords
import org.scalatest.FunSpec

import scala.collection.mutable

/**
  * This work, (c) by jason@ubiquibit.com
  *
  * This work is licensed under a
  * Creative Commons Attribution-ShareAlike 4.0 International License.
  *
  * You should have received a copy of the license along with this
  *   work.  If not, see <http://creativecommons.org/licenses/by-sa/4.0/>.
  */
class InterruptsSpec extends FunSpec with RandomData {

  val stationId = s

  describe("Interrupts case class") {

    it("has only 16 records in the window") {

      val m: mutable.Map[TextRecord, (Set[String], Set[String])] = mutable.Map()
      (0 until 30).foreach { _ => m += rec() -> (Set(s), Set(s)) }

      val instance = Interrupts(stationId, records = m.toMap)

      assert(instance.inWindow().records.size == StationInterrupts.numRecords)
    }

    it("returns the most recent 16 records inWindow") {
      val m: mutable.Map[TextRecord, (Set[String], Set[String])] = mutable.Map()
      (0 until 30).foreach { _ => m += rec() -> (Set(s), Set(s)) }

      val instance = Interrupts(stationId, records = m.toMap)

      val sortedEvents = m.keys.toList.sortWith(sortRecords)

      // sorted latest to earliest
      assert(sortedEvents.head.eventTime after sortedEvents.last.eventTime)

      val keysOutTheWindow = instance.records.keys.filter(k => !sortedEvents.contains(k))

      keysOutTheWindow.foreach(k => assert(k.eventTime before sortedEvents.tail.head.eventTime))

    }

    it("isInterrupted when the second Set[String] is occupied for any frame") {

      val textRecords = testRecords(Some(32)).sortWith(sortRecords)

      val recs = textRecords.map{ tr =>
        tr -> (Set(s), Set[String]())
      }.toMap

      val testInstance = Interrupts(s, recs)

      assert(testInstance.isInterrupted)
    }

    it("isOnlineAgain when the first Set[String] is occupied for any frame"){

      val textRecords = testRecords().sortWith(sortRecords)

      val recs = textRecords.map{ tr =>
        tr -> (Set[String](), Set(s))
      }.toMap

      val testInstnace = Interrupts(s, recs)

      assert(testInstnace.isOnlineAgain)

    }
  }
}
