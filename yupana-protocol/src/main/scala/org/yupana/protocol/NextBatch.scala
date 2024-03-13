/*
 * Copyright 2019 Rusexpertiza LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.yupana.protocol

import org.yupana.serialization.ReadWrite

/**
  * Request for next batch of data
  *
  * @param id request id
  * @param batchSize size of requested batch
  */
case class NextBatch(id: Int, batchSize: Int) extends Command[NextBatch](NextBatch)

object NextBatch extends MessageHelper[NextBatch] {
  override val tag: Tags.Tags = Tags.NEXT
  override val readWrite: ReadWrite[NextBatch] =
    ReadWrite.product2[NextBatch, Int, Int](NextBatch.apply)(x => (x.id, x.batchSize))
}
