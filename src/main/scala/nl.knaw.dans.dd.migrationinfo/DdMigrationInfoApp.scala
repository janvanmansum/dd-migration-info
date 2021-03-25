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
import nl.knaw.dans.lib.dataverse.model.DataverseItem
import nl.knaw.dans.lib.dataverse.model.dataset.DatasetVersion
import nl.knaw.dans.lib.dataverse.model.file.prestaged.{ Checksum, DataFile }
import nl.knaw.dans.lib.error._
import nl.knaw.dans.lib.logging.DebugEnhancedLogging
import resource.managed

import java.sql.{ Connection, SQLException }
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

  def createDataFileRecordsForDataset(datasetDoi: String): Try[Unit] = {
    trace(datasetDoi)

    for {
      r <- dataverse.dataset(datasetDoi).viewAllVersions()
      vs <- r.data
      dfs <- collectUniqueDataFilesFromDataverse(vs)
      _ <- createDataFileRecords(datasetDoi, dfs)
    } yield ()
  }

  def createDataFileRecordsForDataverse(): Try[Unit] = {
    trace(())

    for {
      r <- dataverse.dataverse("root").contents()
      items <- r.data
      _ <- items
        .filter(_.`type` == "dataset")
        .map(getDoiFromContentItem)
        .map(createDataFileRecordsForDataset).collectResults
    } yield ()
  }

  private def getDoiFromContentItem(item: DataverseItem): String = {
    val errors = new ListBuffer[String]()

    if (item.protocol.isEmpty) errors.append("has no protocol")
    if (item.authority.isEmpty) errors.append("has no authority")
    if (item.identifier.isEmpty) errors.append("has no identifier")

    if (errors.nonEmpty) throw new IllegalArgumentException(s"Item $item has no valid persistent identifier: ${ errors.mkString(", ") }")
    else s"${ item.protocol.get }:${ item.authority.get }/${ item.identifier.get }"
  }

  /**
   * Returns all the unique data files for this version sequence. Unless a file is replaced (i.e. its contents is changed)
   * or deleted, the underlying data file is reused in subsequent versions.
   *
   * @param datasetVersions list of dataset versions
   * @return
   */
  private def collectUniqueDataFilesFromDataverse(datasetVersions: List[DatasetVersion]): Try[List[DataFile]] = Try {
    trace(datasetVersions)
    datasetVersions.collect {
      /*
       * Files only in draft versions don't have a dataFile yet, so must be skipped.
       */
      case v => v.files.filter(_.dataFile.isDefined).map(f => f.dataFile.get.toPrestaged).map(pf => (pf.storageIdentifier, pf))
    }.flatten.toMap.values.toList
  }

  private def createDataFileRecords(datasetDoi: String, dataFiles: List[DataFile]): Try[Unit] = {
    trace(datasetDoi, dataFiles)
    dataFiles.map {
      df =>
        for {
          _ <- checkS3storageIdentifier(df.storageIdentifier)
          (bucket, id) <- splitStorageIdentifier(df.storageIdentifier)
          _ <- createDataFileRecord(datasetDoi, getNormalizedStorageIdentifier(bucket, id), df)
        } yield ()
    }.collectResults.map(_ => ())
  }

  def createDataFileRecord(datasetDoi: String, storageId: String, df: DataFile): Try[Unit] = {
    database.doTransaction(implicit c => writeDataFileRecord(datasetDoi, storageId, df))
  }

  private def writeDataFileRecord(datasetDoi: String, storageIdentifier: String, df: DataFile)(implicit c: Connection): Try[Unit] = {
    trace(datasetDoi, storageIdentifier, df)

    managed(c.prepareStatement("INSERT INTO data_file VALUES (?, ?, ?, ?, ?, ?);"))
      .map(prepStatement => {
        prepStatement.setString(1, storageIdentifier)
        prepStatement.setString(2, datasetDoi)
        prepStatement.setString(3, df.fileName)
        prepStatement.setString(4, df.mimeType)
        prepStatement.setString(5, df.checksum.`@value`)
        prepStatement.setLong(6, df.fileSize)
        prepStatement.executeUpdate()
      })
      .tried
      .map(_ => ())
      .recoverWith {
        case e: SQLException if e.getMessage.toLowerCase contains "unique constraint" =>
          Failure(DataFileAlreadyStoredException(df))
      }
  }

  def getDataFileRecordsForDataset(datasetId: String): Try[List[DataFile]] = {
    database.doTransaction(implicit c => readDataFileRecords(datasetId))
  }

  private def readDataFileRecords(datasetDoi: String, optId: Option[String] = Option.empty)(implicit c: Connection): Try[List[DataFile]] = {
    trace(datasetDoi, optId)
    managed(c.prepareStatement("SELECT storage_identifier, file_name, mime_type, sha1_checksum, file_size FROM data_file WHERE dataset_doi = ?;"))
      .map(prepStatement => {
        prepStatement.setString(1, datasetDoi)
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
}
