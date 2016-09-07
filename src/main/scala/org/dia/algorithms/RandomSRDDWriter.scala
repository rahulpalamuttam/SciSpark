/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.algorithms

import org.dia.core.SciSparkContext
import org.dia.core.SRDDFunctions._

object RandomSRDDWriter {

  def main(args: Array[String]): Unit = {
    val master = args(0)
    val hdfspath = args(1)
    val stagepath = args(2)
    val numFiles = args(3).toInt
    val partitions = args(4).toInt
    val varChoices = if (args.length < 6) List(0, 1, 2, 3) else args(5).split(",").map(_.toInt).toList

    val ssc = new SciSparkContext(master, "WriterApp")
    val k = ssc.createRandomSRDD(numFiles, partitions, varChoices)
    k.writeSRDD(hdfspath, stagepath)
  }
}
