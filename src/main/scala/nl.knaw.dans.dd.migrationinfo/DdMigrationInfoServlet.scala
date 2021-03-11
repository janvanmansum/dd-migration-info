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

import nl.knaw.dans.lib.dataverse.model.file.prestaged.DataFile
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import org.json4s.native.JsonMethods
import org.json4s.{ DefaultFormats, Formats }
import org.scalatra._

class DdMigrationInfoServlet(app: DdMigrationInfoApp,
                             version: String) extends ScalatraServlet with DebugEnhancedLogging {

  get("/") {
    contentType = "text/plain"
    Ok(s"Migration Info Service running ($version)")
  }

  get("/datafiles/:storageIdentifier") {
    contentType = "application/json"

    Ok("")
  }

  put("/datafiles/:storageIdentifier") {
    implicit val jsonFormats: Formats = DefaultFormats
    contentType = "application/json"

    val storageId = params("storageIdentifier")
    debug(s"storageId = $storageId")
    debug(s"body = '${request.body}'")
    val dataFile = JsonMethods.parse(request.body).extract[DataFile]
    if (storageId != dataFile.storageIdentifier.split(":").last) BadRequest("Storage identifier in path and request body must be the same")
    else app.createDataFile(dataFile)
      .map(_ => NoContent())
      .recover {
        case e: DataFileAlreadyStoredException => Conflict(e.getMessage)
        case e => InternalServerError(e)
      }.get
  }

  delete("/datafiles/:storageIdentifier") {
    NoContent()
  }
}
