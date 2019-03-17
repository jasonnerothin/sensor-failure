package com.ubiquibit.buoy.jobs

import com.ubiquibit.FakeStationRepository
import com.ubiquibit.buoy._
import org.scalatest.{BeforeAndAfter, FunSpec}

class InitRedisSpec
  extends FunSpec
    with BeforeAndAfter {

  val stationRepository: StationRepository = new FakeStationRepository

  private val fakeRepo: FakeStationRepository = stationRepository.asInstanceOf[FakeStationRepository]

  val fileReckoning: FileReckoning = new FakeFileReckoning()

  private val fakeFilez: FakeFileReckoning = fileReckoning.asInstanceOf[FakeFileReckoning]

  val instance: InitRedis = new InitRedisImpl(this)

  val si0 = StationInfo(StationId.makeStationId("abcd"), 23)
  val si1 = StationInfo(StationId.makeStationId("xyzpdq"), 42)

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

      fakeRepo.readStationsResponse = Seq[StationInfo](si1)
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