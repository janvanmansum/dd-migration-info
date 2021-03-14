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
import org.json4s.native.{ JsonMethods, Serialization }
import org.json4s.{ DefaultFormats, Formats }
import org.scalatra.{ BadRequest, _ }

class DdMigrationInfoServlet(app: DdMigrationInfoApp,
                             version: String) extends ScalatraServlet with DebugEnhancedLogging {
  implicit val jsonFormats: Formats = DefaultFormats

  get("/") {
    contentType = "text/plain"
    Ok(s"Migration Info Service running ($version)")
  }

  get("/datafiles/:bucket/:id") {
    val bucket = params("bucket")
    val id = params("id")
    app.getDataFile(bucket, id)
      .map(_.map(f => Ok(Serialization.writePretty(f)))
        .getOrElse(NotFound("Not Found"))).get
  }

  put("/datafiles/:bucket/:id") {
    contentType = "application/json"

    val bucket = params("bucket")
    val id = params("id")
    debug(s"id = $id")
    debug(s"body = '${ request.body }'")
    val dataFile = JsonMethods.parse(request.body).extract[DataFile]

    splitStorageIdentifier(dataFile.storageIdentifier).map {
      case (bucketFromFile, idFromFile) =>
        if (bucketFromFile != bucket || idFromFile != id)
          BadRequest("Storage identifier in path and request body must be the same")
        else app.createDataFile(bucket, id, dataFile)
          .map(_ => NoContent())
          .recover {
            case e: DataFileAlreadyStoredException => Conflict(e.getMessage)
            case e => InternalServerError(e)
          }.get
    }.get
  }

  delete("/datafiles/:bucket/:id") {
    val bucket = params("bucket")
    val id = params("id")
    app.deleteDataFile(bucket, id).map(_ => NoContent())
      .recover {
        case e: NoSuchElementException => NotFound("No such data file")
        case e => InternalServerError(e)
      }.get
  }

  post("/files/:id/add-record") {
    val fileId = params("id")
    app.addRecordFor(fileId).map(_ => Ok(s"Record added for file $fileId")).get
  }
}
