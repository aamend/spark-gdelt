/**
 * Licensed to Gravity.com under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Gravity.com licenses this file
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
package com.gravity.goose.images

/**
 * Created by Jim Plush
 * User: jim
 * Date: 8/18/11
 */

import javax.imageio.ImageIO
import java.awt.color.CMMException
import java.awt.image.BufferedImage

import com.gravity.goose.utils.{Logging, URLHelper}
import org.apache.http.client.HttpClient
import org.apache.http.HttpEntity
import org.apache.http.protocol.{BasicHttpContext, HttpContext}
import org.apache.http.client.protocol.ClientContext
import java.util.Base64
import java.io._
import java.util

import com.gravity.goose.Configuration
import com.gravity.goose.text.{HashUtils, string}
import org.apache.http.util.EntityUtils
import org.apache.commons.io.IOUtils
import com.gravity.goose.network.{HtmlFetcher, ImageFetchException}

import scala.util.Try
import scala.util.matching.Regex

object ImageUtils extends Logging {
  val spaceRegex: Regex = " ".r
  val xRegex: Regex = "x".r

  /**
  * User: Jim Plush
  * gets the image dimensions for an image file, pass in the path to the image who's dimensions you want to get
  * this will use imageMagick since the Java IO and imaging shit SUCKS for getting mime types and file info for jpg and png files
  *
  * @return
  */
  def getImageDimensions(identifyProgram: String, filePath: String): ImageDetails = {
    val imageInfo = execToString(Array(identifyProgram, filePath))
    val imageDetails: ImageDetails = new ImageDetails
    if (imageInfo == null || imageInfo.contains("no decode delegate for this image format")) {
      throw new IOException("Unable to get Image Information (no decode delegate) for: " + filePath + "\n\tcommand '" + identifyProgram + " " + filePath + "' returned: " + imageInfo)
    }
    val infoParts = spaceRegex.split(imageInfo)
    val mimeType = infoParts.lift(1).getOrElse(string.empty)
    val (width, height) = infoParts.lift(2) match {
      case Some(dimensions) =>
        val pair = xRegex.split(dimensions)
        if (pair.length > 1) {
          val wStr = pair(0)
          val hStr = pair(1)

          (string.tryToInt(wStr).getOrElse(0), string.tryToInt(hStr).getOrElse(0))
        } else {
          (0, 0)
        }
      case None => (0, 0)
    }
    imageDetails.setMimeType(mimeType)
    imageDetails.setWidth(width)
    imageDetails.setHeight(height)
    imageDetails
  }

  def readImageBase64(file: File): String = {
    val fileInputStreamReader = new FileInputStream(file)
    val bytes = new Array[Byte](file.length.asInstanceOf[Int])
    fileInputStreamReader.read(bytes)
    Base64.getEncoder.encodeToString(bytes)
  }

  /**
  * gets the image dimensions for an image file, pass in the path to the image who's dimensions you want to get, uses the built in java commands
  *
  * @return
  */
  def getImageDimensionsJava(filePath: String): util.HashMap[String, Integer] = {
    var image: BufferedImage = null
    try {
      val f: File = new File(filePath)
      image = ImageIO.read(f)
      val results: util.HashMap[String, Integer] = new util.HashMap[String, Integer]
      results.put("height", image.getHeight)
      results.put("width", image.getWidth)
      results
    }
    catch {
      case e: CMMException =>
        logger.error("ERROR READING FILE: " + filePath + " \n", e)
        throw new IOException("Unable to read file: " + filePath)
    }
    finally {
      if (image != null) {
        try {
          image.flush()
        }
        catch {
          case _: Exception =>
        }
      }
    }
  }

  /**
  * Tries to exec the command, waits for it to finish, logs errors if exit
  * status is nonzero, and returns true if exit status is 0 (success).
  *
  * @param command Description of the Parameter
  * @return Description of the Return Value
  */
  private def execToString(command: Array[String]): String = {
    var p: Process = null
    var in: BufferedReader = null
    try {
      p = Runtime.getRuntime.exec(command)
      in = new BufferedReader(new InputStreamReader(p.getInputStream))
      var line: String = null
      line = in.readLine
      p.waitFor
      return line
    }
    catch {
      case e: IOException =>
        logger.error(e.toString, e)
      case e: InterruptedException =>
        logger.error(e.toString, e)
        throw new RuntimeException(e)
    }
    finally {
      if (in != null) {
        try {
          in.close()
        }
        catch {
          case _: IOException =>
        }
      }
      if (p != null) {
        p.destroy()
      }
    }
    null
  }

  /**
  * Writes an image src http string to disk as a temporary file and returns the LocallyStoredImage object that has the info you should need
  * on the image
  */
  def storeImageToLocalFile(httpClient: HttpClient, linkhash: String, imageSrc: String, config: Configuration): Option[LocallyStoredImage] = {

    try {
      // check for a cache hit already on disk
      readExistingFileInfo(linkhash, imageSrc, config) match {
        case Some(locallyStoredImage) =>
          trace("Image already cached on disk: " + imageSrc)
          return Some(locallyStoredImage)
        case None =>
      }

      trace("Not found locally...starting to download image: " + imageSrc)
      fetchEntity(httpClient, imageSrc, config) match {
        case Some(entity) =>
          trace("Got entity for " + imageSrc)
          writeEntityContentsToDisk(entity, linkhash, imageSrc, config) match {
            case Some(locallyStoredImage) => trace("Img Write successfull to disk"); Some(locallyStoredImage)
            case None => trace("Unable to write contents to disk: " + imageSrc); None
          }
        case None => trace("Unable to fetch entity for: " + imageSrc); None
      }
    } catch {
      case e: Exception =>
        info(e, e.toString)
        None
    }

  }


  def readExistingFileInfo(linkhash: String, imageSrc: String, config: Configuration): Option[LocallyStoredImage] = {
    val localImageName = getLocalFileName(linkhash, imageSrc, config)
    val imageFile = new File(localImageName)
    if (imageFile.exists()) {
      try {
        trace("Reading image from disk: " + localImageName)
        val imageDetails = getImageDimensions(config.imagemagickIdentifyPath, localImageName)
        val mimeType = imageDetails.getMimeType.toLowerCase
        val base64 = ImageUtils.readImageBase64(new File(localImageName))
        Some(LocallyStoredImage(imageSrc, mimeType, base64, imageFile.length(), imageDetails.getHeight, imageDetails.getWidth))
      } catch {
        case e: Exception =>
          trace(e, "Unable to get image file dimensions & extension name!")
          None
      }
    } else {
      None
    }

  }

  def writeEntityContentsToDisk(entity: HttpEntity, linkhash: String, imageSrc: String, config: Configuration): Option[LocallyStoredImage] = {

    val localSrcPath = getLocalFileName(linkhash, imageSrc, config)
    val outstream: OutputStream = new FileOutputStream(localSrcPath)
    val instream: InputStream = entity.getContent
     trace("Content Length: " + entity.getContentLength)
    try {
      val fileCopyBytes = IOUtils.copy(instream, outstream)
      trace(fileCopyBytes + " bytes copied to disk")
    } finally {
      Try {
        outstream.flush()
        outstream.close()
        instream.close()
      }
    }
    EntityUtils.consume(entity)
    trace("Content Length: " + entity.getContentLength)
    readExistingFileInfo(linkhash, imageSrc, config)

  }

  def getLocalFileName(linkhash: String, imageSrc: String, config: Configuration): String = {
    val imageHash = HashUtils.md5(imageSrc)
    config.localStoragePath + "/" + linkhash + "_" + imageHash
  }


  def cleanImageSrcString(imgSrc: String): String = spaceRegex.replaceAllIn(imgSrc, "%20")

  def fetchEntity(httpClient: HttpClient, imageSrc: String, config: Configuration): Option[HttpEntity] = {

    URLHelper.tryToHttpGet(imageSrc) match {
      case Some(httpget) =>
        val localContext: HttpContext = new BasicHttpContext
        localContext.setAttribute(ClientContext.COOKIE_STORE, HtmlFetcher.emptyCookieStore)
        val response = try {
          config.getHtmlFetcher.getHttpClient.execute(httpget, localContext)
        }
        catch {
          case ex: Exception => throw new ImageFetchException(imageSrc, ex)
        }

        val respStatus = response.getStatusLine.getStatusCode


        if (respStatus != 200) {
          None
        } else {
          try {
            Option(response.getEntity)
          } catch {
            case e: Exception => warn(e, e.toString); httpget.abort(); None
          }
        }
      case None =>
        warn("Unable to parse imageSrc: '" + imageSrc + "' into HttpGet")
        None
    }

  }


}
