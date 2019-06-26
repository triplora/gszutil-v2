/*
 * Copyright 2019 Google Inc. All Rights Reserved.
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

package com.google.cloud.bqsh

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bqsh.cmd._
import com.google.cloud.gszutil.Util
import com.ibm.jzos.ZFileProvider

import scala.collection.mutable.ListBuffer

object Bqsh {
  def main(args: Array[String]): Unit = {
    val ddName = args.headOption.getOrElse("")
    val zos = ZFileProvider.getProvider()
    val script =
      if (ddName.nonEmpty && zos.ddExists(ddName))
        zos.readDDString(ddName)
      else
        zos.readStdin()

    zos.init()
    Util.configureLogging()
    run(script, sys.env, zos)
  }

  def run(script: String, env: Map[String,String], zos: ZFileProvider, throwOnError: Boolean = true): Result = {
    val env1 = splitSH(script)
      .map(readArgs)
      .foldLeft(env){(a,b) =>
        val result = exec(b, a, zos)
        if (result.exitCode != 0 && throwOnError)
          throw new RuntimeException(result.env.getOrElse("ERRMSG",s"${b.mkString(" ")} returned exit code ${result.exitCode}"))
        a ++ result.env
      }
    Result(env = env1)
  }



  def exec(args: Seq[String], env: Map[String,String], zos: ZFileProvider): Result = {
    lazy val fail = Result.Failure(s"invalid command '${args.mkString(" ")}'")

    def creds: GoogleCredentials = zos
      .getCredentialProvider(ZFileProvider.KeyFileDD)
      .getCredentials

    BqshParser.parse(args, env) match {
      case Some(cmd) =>
        if (cmd.name == "bq"){
          cmd.args.headOption.getOrElse("") match {
            case "mk" =>
              MkOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  Mk.run(c, creds, zos)
                case _ =>
                  fail
              }
            case "query" =>
              QueryOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  Query.run(c, creds, zos)
                case _ =>
                  fail
              }
            case "load" =>
              LoadOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  Load.run(c, creds)
                case _ =>
                  fail
              }
            case "rm" =>
              RmOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  Rm.run(c, creds)
                case _ =>
                  fail
              }
            case _ =>
              fail
          }
        } else if (cmd.name == "gsutil") {
          cmd.args.headOption.getOrElse("") match {
            case "cp" =>
              GsUtilOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  Cp.run(c, creds, zos)
                case _ =>
                  fail
              }
            case "rm" =>
              GsUtilOptionParser.parse(cmd.args) match {
                case Some(c) =>
                  GsUtilRm.run(c, creds)
                case _ =>
                  fail
              }
            case _ =>
              fail
          }
        } else {
          Bqsh.eval(cmd)
        }
      case _ =>
        fail
    }
  }

  def replaceEnvVars(s: String, env: Map[String,String]): String = {
    var variable = false
    var bracket = false
    var singleQuoted = false
    var doubleQuoted = false
    val chars = s.toCharArray.toSeq
    var i = 0
    val varName = new StringBuilder(64)
    val sb = new StringBuilder(s.length)
    var c: Char = 0
    var next = c
    while (i < chars.length) {
      c = chars(i)
      if (i + 1 < chars.length) {
        next = chars(i + 1)
      } else {
        next = ' '
      }

      if (variable || bracket) {
        if (bracket && next == '}') {
          variable = false
          bracket = false
          if (!(c.isLetterOrDigit || c == '_')) {
            throw new IllegalArgumentException("invalid variable")
          }
          varName.append(c)
          sb.append(env.getOrElse(varName.result(), ""))
          varName.clear()
        } else if (!(next.isLetterOrDigit || next == '_')) {
          variable = false
          if (!(c.isLetterOrDigit || c == '_')) {
            throw new IllegalArgumentException("invalid variable")
          }
          varName.append(c)
          sb.append(env.getOrElse(varName.result(), ""))
          varName.clear()
        } else {
          varName.append(c)
        }
      } else {
        if (!(singleQuoted || doubleQuoted)) {
          if (c == '\'') {
            singleQuoted = true
          } else if (c == '"') {
            doubleQuoted = true
          } else if (c == '\\' && next == '$') {
            i += 1
            sb.append(next)
          } else if (c == '$' && next == '{') {
            variable = true
            bracket = true
            i += 1
          } else if (c == '$') {
            variable = true
          } else {
            sb.append(c)
          }
        } else {
          if (singleQuoted && c == '\'') {
            singleQuoted = false
          } else if (doubleQuoted && c == '"') {
            doubleQuoted = false
          } else if (c == '\\' && next == '$') {
            i += 1
            sb.append(next)
          } else if (!singleQuoted && c == '$' && next == '{') {
            variable = true
            bracket = true
            i += 1
          } else if (!singleQuoted && c == '$') {
            variable = true
          } else {
            sb.append(c)
          }
        }
      }
      i += 1
    }
    if (singleQuoted)
      throw new IllegalArgumentException("unmatched single quote")
    if (doubleQuoted)
      throw new IllegalArgumentException("unmatched double quote")
    if (bracket)
      throw new IllegalArgumentException("unmatched bracket")
    sb.result()
  }

  def readArgs(s: String): Seq[String] = {
    var singleQuoted = false
    var doubleQuoted = false
    val chars = s.toCharArray.toSeq
    var i = 0
    val l = new ListBuffer[String]()
    val sb = new StringBuilder(1024)
    var c: Char = 0
    var next: Char = 0
    while (i < chars.length) {
      c = chars(i)
      next = chars.lift(i+1).getOrElse('\n')
      if (doubleQuoted || singleQuoted){
        if (c == '"' && doubleQuoted){
          doubleQuoted = false
        } else if (c == '\'' && singleQuoted) {
          singleQuoted = false
        } else if (c != '\n'){
          sb.append(c)
        }
      } else {
        if (c == '"'){
          doubleQuoted = true
        } else if (c == '\'') {
          singleQuoted = true
        } else if (c == ' ' || c == '\t') {
          if (sb.nonEmpty) {
            l += sb.result()
            sb.clear()
          }
        } else if (c == '\\' && next == '\n') {
          i += 1
        } else {
          sb.append(c)
        }
      }

      i += 1
    }
    if (sb.nonEmpty){
      l += sb.result()
      sb.clear()
    }
    l.result()
  }

  def splitSH(s: String): Seq[String] = {
    var commentLine = false
    var singleQuoted = false
    var doubleQuoted = false
    val chars = s.toCharArray.toSeq
    var i = 0
    val l = new ListBuffer[String]()
    val sb = new StringBuilder(1024)
    var c: Char = 0
    var next = c
    while (i < chars.length) {
      c = chars(i)
      if (i+1 < chars.length) {
        next = chars(i+1)
      } else {
        next = ';'
      }
      if (!commentLine) {
        if ((c == ';' || c == '\n' || c == '#') && !(singleQuoted || doubleQuoted)) {
          val result = sb.result().trim
          if (result.nonEmpty)
            l += result
          sb.clear()
        }

        if (!(singleQuoted || doubleQuoted)) {
          if (c == '#') {
            commentLine = true
          } else if (c == '\'') {
            singleQuoted = true
          } else if (c == '"') {
            doubleQuoted = true
          } else if (c == '\\' && next == '\n') {
            i += 1
          }
        } else {
          if (singleQuoted && c == '\'') {
            singleQuoted = false
          } else if (doubleQuoted && c == '"') {
            doubleQuoted = false
          }
        }
        if (!commentLine && c != '\\')
          sb.append(c)
      } else {
        if (commentLine && c == '\n') {
          commentLine = false
        }
      }
      i += 1
    }
    if (sb.nonEmpty){
      val result = sb.result().trim
      sb.clear()
      if (result.nonEmpty)
        l += result
    }
    if (singleQuoted)
      throw new IllegalArgumentException("unmatched single quote")
    if (doubleQuoted)
      throw new IllegalArgumentException("unmatched double quote")
    l.result()
  }

  def splitSQL(s: String): Seq[String] = {
    var commentLine = false
    var commentBlock = false
    var singleQuoted = false
    var doubleQuoted = false
    val chars = s.toCharArray.toSeq
    var i = 0
    val l = new ListBuffer[String]()
    val sb = new StringBuilder(1024)
    var c = 0.toChar
    var next = c
    while (i < chars.length) {
      c = chars(i)
      if (i+1 < chars.length) {
        next = chars(i+1)
      } else {
        next = ';'
      }
      if (!commentLine && !commentBlock) {
        if (c == ';' && !singleQuoted && !doubleQuoted) {
          val result = sb.result().trim
          if (result.nonEmpty)
            l += result
          sb.clear()
        } else {
          sb.append(c)
        }

        if (!singleQuoted && !doubleQuoted) {
          if (c == '-' && next == '-') {
            commentLine = true
            i += 1
            sb.append(next)
          } else if (c == '/' && next == '*') {
            commentBlock = true
            i += 1
            sb.append(next)
          } else if (c == '\'') {
            singleQuoted = true
          } else if (c == '"') {
            doubleQuoted = true
          }
        } else {
          if (singleQuoted && c == '\'') {
            singleQuoted = false
          } else if (doubleQuoted && c == '"') {
            doubleQuoted = false
          }
        }
      } else {
        sb.append(c)
        if (commentLine && c == '\n') {
          commentLine = false
        } else if (commentBlock && c == '*' && next == '/') {
          commentBlock = false
          i += 1
          sb.append(next)
        }
      }

      i += 1
    }
    l.result()
  }

  def eval(cmd: ShCmd): Result = {
    if (cmd.name == "echo") {
      System.out.println(cmd.args.mkString(" "))
      Result.Success
    } else {
      val i = cmd.name.indexOf('=')
      if (i > 0) {
        val varName = cmd.name.substring(0, i)
        val value = cmd.name.substring(i + 1)
        Result.withExport(varName, value)
      } else {
        Result.Failure(s"${cmd.name}: command not found")
      }
    }
  }
}
