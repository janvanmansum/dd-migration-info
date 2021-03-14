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

import nl.knaw.dans.lib.dataverse.DataverseInstance
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion
import nl.knaw.dans.lib.dataverse.model.file.FileMeta
import nl.knaw.dans.lib.dataverse.model.file.prestaged.{ Checksum, DataFile }
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import nl.knaw.dans.lib.error._
import resource.managed

import java.sql.{ Connection, SQLException }
import scala.collection.mutable
import scala.util.{ Failure, Try }

class DdMigrationInfoApp(configuration: Configuration) extends DebugEnhancedLogging {
  val database = new Database(
    url = configuration.databaseUrl,
    user = configuration.databaseUser,
    password = configuration.databasePassword,
    driver = configuration.databaseDriver)
  val dataverse = new DataverseInstance(configuration.dataverse)

  logger.info("Initializing database connection...")
  database.initConnectionPool()
  logger.info("Database connection initialized.")

  def createDataFile(bucket: String, storageId: String, df: DataFile): Try[Unit] = {
    database.doTransaction(implicit c => writeDataFileRecord(bucket, storageId, df))
  }

  private def writeDataFileRecord(bucket: String, id: String, df: DataFile)(implicit c: Connection): Try[Unit] = {
    trace(bucket, id, df)

    managed(c.prepareStatement("INSERT INTO data_file VALUES (?, ?, ?, ?, ?);"))
      .map(prepStatement => {
        prepStatement.setString(1, getNormalizedStorageIdentifier(bucket, id))
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

  private def getNormalizedStorageIdentifier(bucket: String, id: String): String = {
    s"s3://${ bucket.toLowerCase }:${ id.toLowerCase }"
  }

  def getDataFile(bucket: String, storageId: String): Try[Option[DataFile]] = {
    trace(storageId)
    database.doTransaction(implicit c => readDataFileRecord(bucket, storageId)).map(_.headOption)
  }

  private def readDataFileRecord(bucket: String, id: String)(implicit c: Connection): Try[List[DataFile]] = {
    trace(bucket, id)
    managed(c.prepareStatement("SELECT storage_identifier, file_name, mime_type, sha1_checksum, file_size FROM data_file WHERE storage_identifier = ?;"))
      .map(prepStatement => {
        prepStatement.setString(1, getNormalizedStorageIdentifier(bucket, id))
        prepStatement.executeQuery()
      })
      .map(r => {
        val dataFiles = new mutable.ListBuffer[DataFile]()

        while (r.next()) {
          dataFiles.append(
            DataFile(
              storageIdentifier = r.getString("storage_identifier"),
              fileName = r.getString("file_name"),
              mimeType = r.getString("mime_type"),
              checksum = Checksum(
                `@type` = "SHA-1",
                `@value` = r.getString("sha1_checksum")
              ),
              fileSize = r.getLong("file_size"))
          )
        }
        dataFiles.toList
      }).tried
  }

  def deleteDataFile(bucket: String, id: String): Try[Unit] = {
    database.doTransaction(implicit c => deleteDataFileRecord(bucket, id))
  }

  private def deleteDataFileRecord(bucket: String, id: String)(implicit c: Connection): Try[Unit] = {
    trace(bucket, id)
    managed(c.prepareStatement("DELETE FROM data_file WHERE storage_identifier = ?"))
      .map(prepStatement => {
        prepStatement.setString(1, getNormalizedStorageIdentifier(bucket, id))
        val n = prepStatement.executeUpdate()
        if (n == 0) throw new NoSuchElementException("Record not found")
      })
      .tried
      .map(_ => ())
  }

  def addRecordsFor(datasetId: String): Try[Unit] = {
    trace(datasetId)

    for {
      r <- if (datasetId.forall(_.isDigit)) dataverse.dataset(datasetId.toInt).viewAllVersions()
           else dataverse.dataset(datasetId).viewAllVersions()
      vs <- r.data
      dfs <- collectUniqueDataFiles(vs)
      _ <- createDataFiles(dfs)
    } yield ()
  }

  /**
   * Returns all the unique data files for this version sequence. Unless a file is replaced (i.e. its contents is changed)
   * or deleted, the underlying data file is reused in subsequent versions.
   *
   * @param datasetVersions list of dataset versions
   * @return
   */
  private def collectUniqueDataFiles(datasetVersions: List[DatasetVersion]): Try[List[DataFile]] = Try {
    trace(datasetVersions)
    datasetVersions.collect {
      case v => v.files.map(f => f.dataFile.get.toPrestaged).map(pf => (pf.storageIdentifier, pf))
    }.flatten.toMap.values.toList
  }

  private def createDataFiles(dataFiles: List[DataFile]): Try[Unit] = {
    trace(dataFiles)
    dataFiles.map {
      df =>
        for {
          (bucket, id) <- splitStorageIdentifier(df.storageIdentifier)
          _ <- createDataFile(bucket, id, df)
        } yield ()
    }.collectResults.map(_ => ())
  }
}
