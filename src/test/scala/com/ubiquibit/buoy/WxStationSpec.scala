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

package com.ubiquibit.buoy

import com.ubiquibit.TimeHelper
import org.scalatest.FunSpec
import org.scalatest.words.ShouldVerb

class WxStationSpec extends FunSpec with ShouldVerb{

  private val statId0 = StationId.makeStationId("asdb")
  private val statId1 = StationId.makeStationId("zyzuser")

  val simple = WxStation(statId0, 123)

  val withFeeds = WxStation(statId1, 234, TimeHelper.epochTimeZeroUTC().toString, Map(Rain -> KAFKALOADED, Ocean -> DOWNLOADED))
  private val withFeedsAsMap = Map[String, String](
    WxStation.stationIdKey -> statId1.toString,
    WxStation.reportFrequencyKey -> withFeeds.reportFrequencyMinutes.toString,
    Rain.toString -> KAFKALOADED.toString, Ocean.toString -> DOWNLOADED.toString
  )

  describe("StationInfo should") {

    it("map with toMap") {

      val s = simple.toMap
      assert(s(WxStation.stationIdKey) === statId0.toString)
      assert(s(WxStation.reportFrequencyKey) === "123")

      val t = withFeeds.toMap
      assert(t(WxStation.stationIdKey) === statId1.toString)
      assert(t(WxStation.reportFrequencyKey) === "234")
      assert(t(Rain.toString.toUpperCase) === KAFKALOADED.toString.toUpperCase)
      assert(t(Ocean.toString.toUpperCase) === DOWNLOADED.toString.toUpperCase)

    }

    it("instantiate with valueOf") {

      val result: WxStation = WxStation.valueOf(withFeedsAsMap).get

      assert(result === withFeeds)

    }
  }

}
