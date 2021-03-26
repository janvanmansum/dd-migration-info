/**
 * Copyright (C) 2021 DANS - Data Archiving and Networked Services (info@dans.knaw.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.knaw.dans.dd.migrationinfo

import nl.knaw.dans.lib.dataverse.DataverseException
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.native.Serialization
import org.json4s.{ DefaultFormats, Formats }
import org.scalatra._

import scala.util.control.NonFatal

class DdMigrationInfoServlet(app: DdMigrationInfoApp,
                             version: String) extends ScalatraServlet with DebugEnhancedLogging {
  implicit val jsonFormats: Formats = DefaultFormats

  get("/") {
    contentType = "text/plain"
    Ok(s"Migration Info Service running ($version)")
  }

  // TODO: convert to getDatasetDoi. If a database ID, the DOI must be looked up in Dataverse
  private def getDatasetId: String = {
    if (params("id") == ":persistentId") params("persistentId")
    else params("id")
  }

  post("/datasets/:id/datafiles/actions/load-from-dataverse") {
    val datasetId = getDatasetId
    app.createDataFileRecordsForDataset(datasetId).map(_ => Ok(s"Records added for dataset $datasetId"))
      .doIfFailure {
        case NonFatal(e) => logger.warn(s"Exception when creating DataFile records for dataset $datasetId")
      }
      .recover {
        case e: DataverseException if e.status == 404 => NotFound(s"No such dataset: ${ datasetId }")
        case NonFatal(e) => InternalServerError(e.getMessage)
      }.get
  }

  post("/datasets/actions/load-from-dataverse") {
    app.createDataFileRecordsForDataverse()
      .map(_ => Ok("Records added for dataverse root"))
      .doIfFailure {
        case NonFatal(e) => logger.warn("Errors when loading data file records from dataverse root", e)
      }
      .recover {
        case NonFatal(e) => InternalServerError(e.getMessage)
      }
      .get
  }

  get("/datasets/:id/datafiles") {
    val datasetId = getDatasetId
    app.getDataFileRecordsForDataset(datasetId)
      .map(dfs => Ok(Serialization.writePretty(dfs))).get
  }
}
