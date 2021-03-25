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
package nl.knaw.dans.dd

import scala.util.{ Failure, Try, Success }

package object migrationinfo {
  type Bucket = String
  type Id = String

  def checkS3storageIdentifier(si: String): Try[Unit] = {
    if (si matches """^s3://(.*):(.*)$""") Success(())
    else Failure(new IllegalArgumentException(s"Not a valid S3 storage identifier: $si"))
  }

  // TODO: refactor to re-use regex instead of split
  def splitStorageIdentifier(si: String): Try[(Bucket, Id)] = {
    val parts = si.split(":|s3://").filterNot(_.isEmpty)
    if (parts.length != 2) Failure(new IllegalArgumentException(s"Not a valid storageIdentifier: $si"))
    else Success(parts(0), parts(1))
  }

  def getNormalizedStorageIdentifier(bucket: String, id: String): String = {
    s"s3://${ bucket.toLowerCase }:${ id.toLowerCase }"
  }
}
