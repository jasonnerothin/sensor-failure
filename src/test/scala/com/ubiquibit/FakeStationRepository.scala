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

package com.ubiquibit

import java.sql.Timestamp

import com.ubiquibit.buoy._

class FakeStationRepository extends StationRepository {

  def reset{
    readStationsCount = 0
    readStationsResponse = Seq()
    deleteCount = 0
    saveCount = 0
    updateLastReportCount = 0
  }

  var readStationsResponse: Seq[WxStation] = Seq()

  var readStationsCount = 0

  override def readStations(): Seq[WxStation] = {
    readStationsCount = readStationsCount + 1
    readStationsResponse
  }

  override def updateImportStatus(stationId: StationId, buoyData: WxFeed, importStatus: WxFeedStatus): Option[WxFeedStatus] = throw new UnsupportedOperationException("updateImportStatus")

  override def getImportStatus(stationId: StationId, buoyData: WxFeed): Option[WxFeedStatus] = throw new UnsupportedOperationException("getImportStatus")

  var deleteCount = 0

  override def deleteStations(): Boolean = {
    deleteCount = deleteCount + 1
    true
  }

  override def readStation(stationId: StationId): Option[WxStation] = ???

  var saveCount = 0

  override def saveStation(stationInfo: WxStation): Option[StationId] = {
    saveCount = saveCount + 1
    Some(stationInfo.stationId)
  }

  var updateLastReportCount = 0
  override def updateLastReport(stationId: StationId, time: Timestamp): Unit = {
    updateLastReportCount = updateLastReportCount + 1
  }
}
