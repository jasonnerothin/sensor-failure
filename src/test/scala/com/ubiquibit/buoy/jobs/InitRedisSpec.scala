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

import com.ubiquibit.FakeStationRepository
import com.ubiquibit.buoy._
import com.ubiquibit.buoy.jobs.setup.{InitRedis, InitRedisImpl}
import org.scalatest.{BeforeAndAfter, FunSpec}

class
InitRedisSpec
  extends FunSpec
    with BeforeAndAfter {

  val stationRepository: StationRepository = new FakeStationRepository

  private val fakeRepo: FakeStationRepository = stationRepository.asInstanceOf[FakeStationRepository]

  val fileReckoning: FileReckoning = new FakeFileReckoning()

  private val fakeFilez: FakeFileReckoning = fileReckoning.asInstanceOf[FakeFileReckoning]

  val instance: InitRedis = new InitRedisImpl(this)

  val si0 = WxStation(StationId.makeStationId("abcd"), 23)
  val si1 = WxStation(StationId.makeStationId("xyzpdq"), 42)

  after {

    fakeRepo.reset
    fakeFilez.reset

  }

  describe("InitRedis should") {

    it("do NOT MUCH when station count in redis and on disk are the same") {

      fakeRepo.readStationsResponse = Seq(si0, si1)
      fakeFilez.fakeStationIds = Seq(si0.stationId, si1.stationId)

      instance.run()

      assert(fakeFilez.stationIdCount === 1) // 0
      assert(fakeRepo.readStationsCount === 2)
      assert(fakeRepo.saveCount === 0)

    }

    it("initialize if # stations in redis is < # stations on disk") {

      fakeRepo.readStationsResponse = Seq[WxStation](si1)
      fakeFilez.fakeStationInfo = Seq(si0, si1)

      instance.run()

      assert(fakeFilez.stationIdCount === 1)
      assert(fakeRepo.deleteCount === 1)
      assert(fakeRepo.saveCount === 2)

    }

    it("saves once if DB is empty") {

      fakeRepo.readStationsResponse = Seq()
      fakeFilez.fakeStationInfo = Seq(si0, si1)

      instance.run()

      assert(fakeFilez.stationIdCount === 1)
      assert(fakeRepo.saveCount === 2)
      assert(fakeRepo.deleteCount === 0)

    }

  }


}