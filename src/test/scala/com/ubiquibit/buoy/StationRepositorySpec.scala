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

import com.redis.RedisClient
import com.ubiquibit._
import org.scalatest.{BeforeAndAfter, FunSpec}
import com.ubiquibit.buoy.StationId.makeStationId

import scala.collection.immutable.HashMap

class StationRepositorySpec extends FunSpec with BeforeAndAfter {

  val client: RedisClient = new FakeRedisClient
  private val fakeClient = client.asInstanceOf[FakeRedisClient]
  val redis: Redis = new FakeRedis(client)

  val instance: StationRepository = new StationRepositoryImpl(this)

  after {
    fakeClient.reset()
  }

  private val stationId0 = StationId.makeStationId("abcdefg")
  private val station0type0 = Ocean
  private val station0type1 = Adcp2
  private val station0Info = WxStation(stationId0, 0, feeds = Map[WxFeed, WxFeedStatus](station0type0 -> DOWNLOADED, station0type1 -> UNSUPPORTED))

  private val stationId1 = StationId.makeStationId("xyqpdq")
  private val station1type0 = Text

  private val station1ReadyResponse = HashMap[String, String](station1type0.toString.toUpperCase -> DOWNLOADED.toString)

  describe("StationRepository should") {

    it("delete stations from redis") {

      fakeClient.fakeKeys = Some(List(Some(s"stationId:$stationId0"))) //, Some(s"stationId:$station1")))
      fakeClient.fakeHmgetResult = station0Info.toMap //, station1Info.toMap.asInstanceOf[Map[Any, String]])

      instance.deleteStations()

      assert(fakeClient.keysCount === 1)
      assert(fakeClient.getCount === 1)
      assert(fakeClient.delCount === 1)

    }

    it("return import status") {

      val mt = instance.getImportStatus(makeStationId("aadbkasjdgbj"), WxFeed.values.head)
      assert(mt === None)
      assert(fakeClient.getCount === 1)

      fakeClient.fakeHmgetResult = HashMap(station0type0.toString.toUpperCase -> DOWNLOADED.toString, station0type1.toString.toUpperCase -> DOWNLOADED.toString)
      val result0 = instance.getImportStatus(stationId0, station0type0)
      assert(result0.isDefined)
      assert(fakeClient.getCount === 2)
      val status = result0.get
      assert(status === DOWNLOADED)

      fakeClient.fakeHmgetResult = station1ReadyResponse
      val result1 = instance.getImportStatus(stationId1, Ocean)
      assert(result1 === None)
      assert(fakeClient.getCount == 3)

    }

    it("update import status") {

      val result0 = instance.updateImportStatus(makeStationId("abababa"), WxFeed.values.head, DOWNLOADED)
      assert(fakeClient.keysCount === 1)
      assert(fakeClient.setCount === 0)
      assert(result0 === None)

      fakeClient.reset()
      fakeClient.fakeHmgetResult = station1ReadyResponse
      fakeClient.fakeKeys = Some(List(Some(stationId1.toString)))
      val result1 = instance.updateImportStatus(stationId1, station1type0, KAFKALOADED)
      assert(fakeClient.keysCount === 1)
      assert(fakeClient.setCount === 1)

    }

    it("reads station info from redis") {

      val s0 = WxStation(stationId1, 0, TimeHelper.epochTimeZeroUTC().toString, Map(Adcp -> DOWNLOADED, Adcp2 -> ERROR))
      val s1 = WxStation(stationId0, 0, TimeHelper.epochTimeZeroUTC().toString, Map(Text -> KAFKALOADED, Hkp -> DOWNLOADED))

      fakeClient.fakeKeys = Some(List(Some(StationRepository.redisKey(stationId0)), Some(StationRepository.redisKey(stationId1))))
      fakeClient.fakeHmgetResult = s0.toMap //, s1.toMap)

      val result0 = instance.readStations()

      assert(fakeClient.keysCount === 1)

      assert(result0.length === 2)

    }

  }

}
