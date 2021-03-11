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
import resource.managed

import java.sql.{ Connection, SQLException }
import scala.util.{ Failure, Try }

class DdMigrationInfoApp(configuration: Configuration) extends DebugEnhancedLogging {
  val database = new Database(
    url = configuration.databaseUrl,
    user = configuration.databaseUser,
    password = configuration.databasePassword,
    driver = configuration.databaseDriver)

  logger.info("Initializing database connection...")
  database.initConnectionPool()
  logger.info("Database connection initialized.")

  def createDataFile(df: DataFile): Try[Unit] = {
    database.doTransaction(implicit c => writeDataFileRecord(df))
  }

  private def writeDataFileRecord(df: DataFile)(implicit c: Connection): Try[Unit] = {
    trace(df)

    managed(c.prepareStatement("INSERT INTO data_file VALUES (?, ?, ?, ?, ?);"))
      .map(prepStatement => {
        prepStatement.setString(1, df.storageIdentifier)
        prepStatement.setString(2, df.fileName)
        prepStatement.setString(3, df.mimeType)
        prepStatement.setString(4, df.checksum.`@value`)
        prepStatement.setLong(5, df.fileSize)
        prepStatement.executeUpdate()
      })
      .tried
      .map(_ => ())
      .recoverWith {
        case e: SQLException if e.getMessage.toLowerCase contains "unique constraint" =>
          Failure(DataFileAlreadyStoredException(df))
      }
  }

  def getDataFile(storageId: String): Try[DataFile] = {
    trace(storageId)
    database.doTransaction(implicit c => readDataFileRecord(storageId))
  }

  private def readDataFileRecord(storageIdentifier: String)(implicit c: Connection): Try[DataFile] = {
    trace(storageIdentifier)
    managed(c.prepareStatement("SELECT file_name, mime_type, sha1_checksum, file_size FROM data_file WHERE storage_identifier = ?;"))
      .map(prepStatement => {
        prepStatement.setString(1, storageIdentifier)
        prepStatement.executeQuery()
      })
      .tried
      .map(_ => ())
      .recoverWith {
        case e: SQLException if e.getMessage.toLowerCase contains "unique constraint" =>
          Failure(DataFileAlreadyStoredException(df))
      }
  }

}
