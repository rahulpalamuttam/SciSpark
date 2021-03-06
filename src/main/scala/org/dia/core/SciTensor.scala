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
package org.dia.core

import java.io.Serializable
import java.util

import scala.collection.mutable
import scala.language.implicitConversions

import ucar.ma2
import ucar.nc2.Attribute
import ucar.nc2.NetcdfFileWriter

import org.dia.tensors.AbstractTensor

/**
 * The SciTensor is a self-documented array object. It stores N number of variable arrays.
 * The SciTensor also keeps track of a metadata table for properties which the user may want to record.
 * Note that all linear algebra and ocw operations on SciTensors are performed on the variable in use.
 * Furthermore SciTensors are treated as immutable objects and so all operations return new SciTensor objects.
 *
 * @param variables A hashmap of variable name to the tensor of variable values.
 */
class SciTensor(val variables: mutable.Map[String, AbstractTensor]) extends Serializable {

  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val metaData = new mutable.HashMap[String, String]
  var varInUse = variables.head._1

  def this(variableName: String, array: AbstractTensor) {
    this(new mutable.LinkedHashMap[String, AbstractTensor] += ((variableName, array)))
  }

  def this(variableName: String, array: AbstractTensor, metaDataVar: (String, String)*) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  /**
   * Reshapes the array and inserts the reshaped array into the variable hashmap.
   * If a new name is not specified then the variable in use is used by default.
   * The AbstractTensor which corresponds to the variable in use is replaced by
   * the reshaped one.
   *
   * @param reshapedVarName The new variable name. Default is the current variable in use.
   * @param shape           The array specifying dimensions of the new shape
   */
  def reshape(shape: Array[Int], reshapedVarName: String = varInUse): SciTensor = {
    insertVar(reshapedVarName, variables(varInUse).reshape(shape))
    this
  }

  /**
   * Writes metaData in the form of key-value pairs
   */
  def insertDictionary(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) metaData += variable
  }

  /**
   * Insert a variable with its values into the SciTensor.
   *
   * Note that this overwrites the variable in case it already exists.
   */
  def insertVar(varName: String, varTensor: AbstractTensor): Unit = {
    variables.put(varName, varTensor)
  }

  /**
   * Returns the array corresponding to the variable in use.
   * This is to mimic the numpy like syntax of nc['var'][:]
   * Example usage: val absT = sciT('var')()
   *
   * @return AbstractTensor corresponding to variable in use
   */
  def apply(): AbstractTensor = variables(varInUse)


  /**
   * Slices the head variable array given the list of ranges per dimension.
   */
  def apply(ranges: (Int, Int)*): SciTensor = {
    variables(varInUse)(ranges: _*)
  }

  /**
   * Shifts the variable in use pointer to a different variable array.
   * If the variable is not found, error message to log4j.
   */
  def apply(variable: String): SciTensor = {
    if (variables.keySet.contains(variable)) {
      varInUse = variable
    } else {
      LOG.error("Variable " + variable + " was NOT FOUND in the variable array table.")
    }
    this
  }

  /**
   * Returns the variable array that is currently being used
   */
  def tensor: AbstractTensor = variables(varInUse)

  /**
   * Linear Algebra Operations
   */
  def **(other: SciTensor): SciTensor = this.tensor ** other.tensor

  def +(other: SciTensor): SciTensor = this.tensor + other.tensor

  def +(scalar: Double): SciTensor = this.tensor + scalar

  def -(other: SciTensor): SciTensor = this.tensor - other.tensor

  def -(scalar: Double): SciTensor = this.tensor - scalar

  def /(other: SciTensor): SciTensor = this.tensor / other.tensor

  def /(scalar: Double): SciTensor = this.tensor / scalar

  def *(other: SciTensor): SciTensor = this.tensor * other.tensor

  def *(scalar: Double): SciTensor = this.tensor * scalar

  def +=(other: SciTensor): SciTensor = this.tensor += other.tensor

  def +=(scalar: Double): SciTensor = this.tensor += scalar

  def -=(other: SciTensor): SciTensor = this.tensor -= other.tensor

  def -=(scalar: Double): SciTensor = this.tensor -= scalar

  def /=(other: SciTensor): SciTensor = this.tensor /= other.tensor

  def /=(scalar: Double): SciTensor = this.tensor /= scalar

  def *=(other: SciTensor): SciTensor = this.tensor *= other.tensor

  def *=(scalar: Double): SciTensor = this.tensor *= scalar


  /**
   * Applies a masking function on the current variable array
   */
  def mask(f: Double => Boolean, maskVal: Double = 0.0): SciTensor = variables(varInUse).mask(f, maskVal)

  /**
   * Sets the default mask value for the particular array being used.
   */
  def setMask(num: Double): SciTensor = variables(varInUse).setMask(num)

  /**
   * Masks the current variable array by preserving values
   * less than or equal to num.
   */
  def <=(num: Double): SciTensor = variables(varInUse) <= num

  /**
   * Masks the current variable array by preserving values
   * greater than or equal to num.
   */
  def >=(num: Double): SciTensor = variables(varInUse) >= num

  /**
   * Masks the current variable array by preserving values
   * less than to num.
   */
  def <(num: Double): SciTensor = variables(varInUse) < num

  /**
   * Masks the current variable array by preserving values
   * greater than num.
   */
  def >(num: Double): SciTensor = variables(varInUse) > num

  /**
   * Masks the current variable array by preserving values
   * not equal to num.
   */
  def !=(num: Double): SciTensor = variables(varInUse) != num

  /**
   * Masks the current variable array by preserving values
   * equal to num.
   */
  def :=(num: Double): SciTensor = variables(varInUse) := num

  /**
   * Returns the data as a flattened array and the dimensions
   *
   */
  def shape: Array[Int] = variables(varInUse).shape

  def data: Array[Double] = variables(varInUse).data

  /**
   * Creates a copy of the variable in use
   *
   * @return
   */
  def copy: SciTensor = variables(varInUse).copy

  /**
   * Statistical operations
   */

  /**
   * Computes the mean along the given axis of the variable in use.
   *
   * @param axis the axis to take the mean along (can be more than one axis)
   * @return the reduced array with means taken along the specified dimension(s)
   */
  def mean(axis: Int*): SciTensor = {
    variables(varInUse).mean(axis: _*)
  }

  /**
   * Computes and returns the array broadcasted to
   * the specified shape requirements.
   *
   * @param shape the new shape to be broadcasted to
   * @return
   */
  def broadcast(shape: Array[Int]): SciTensor = {
    variables(varInUse).broadcast(shape)
  }

  def detrend(axis: Array[Int]): SciTensor = {
    variables(varInUse).detrend(0)
  }

  def std(axis: Array[Int]): SciTensor = {
    variables(varInUse).std(axis: _*)
  }

  def skew(axis: Array[Int]): SciTensor = {
    variables(varInUse).skew(axis: _ *)
  }

  /**
   * Returns a block averaged tensor where the blocks are squares with
   * dimensions blockInt.
   */
  def reduceResolution(blockInt: Int, invalid: Double = Double.NaN): SciTensor = {
    variables(varInUse).reduceResolution(blockInt, invalid)
  }

  /**
   * ------------------------------ Matrix Operations ---------------------------------
   * The following functions are Matrix Operations specific to SciSpark and it's goals.
   */

  /**
   * Returns a block averaged matrix where the blocks are rectangles with dimensions
   * rowblockSize X colblockSize.
   */
  def reduceRectangleResolution(rowblockSize: Int, colblockSize: Int, invalid: Int): SciTensor = {
    variables(varInUse).reduceRectangleResolution(rowblockSize, colblockSize, invalid)
  }

  def writeToNetCDF(name: String, path: String = ""): Unit = {
    val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, path + name, null)
    val netcdfKeyValue = variables.map {
      case (key, variable) =>
        val dims = new util.ArrayList[ucar.nc2.Dimension]()
        val shape = variable.shape
        for (i <- shape.indices) {
          val newDim = writer.addDimension(null, i.toString, shape(i))
          dims.add(newDim)
        }
        val varT = writer.addVariable(null, key, ma2.DataType.FLOAT, dims)

        val dataOut = ma2.Array.factory(ma2.DataType.DOUBLE, variable.shape, variable.data)
        (varT, dataOut)
    }

    for ((key, attribute) <- metaData) {
      writer.addGroupAttribute(null, new Attribute(key, attribute))
    }

    writer.create()
    for ((variable, array) <- netcdfKeyValue) writer.write(variable, array)
    writer.close()
  }

  override def equals(any: Any): Boolean = {
    val otherSciT = any.asInstanceOf[SciTensor]
    variables == otherSciT.variables &&
    metaData == otherSciT.metaData
  }

  override def hashCode(): Int = super.hashCode()

  override def toString: String = {
    var string = "Variable in use = " + varInUse + "\n" + variables.keys.toString + "\n"
    metaData.foreach(string += _ + "\n")
    string
  }

  /**
   * An implicit converter that is called on every SciTensor operator function.
   */
  private implicit def convert(tensor: AbstractTensor): SciTensor = new SciTensor(varInUse, tensor, metaData)

  def this(variableName: String, array: AbstractTensor, metaDataVar: mutable.HashMap[String, String]) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

}
