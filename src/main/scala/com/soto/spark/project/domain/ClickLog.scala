package com.soto.spark.project.domain

/**
  *
  * @param ip 日志访问ip地址
  * @param time  日志访问时间
  * @param courseId 日志访问实战课程编号
  * @param statusCode 日志访问状态码
  * @param refer 日志访问refer
  */
case class ClickLog(ip: String, time: String, courseId: Int, statusCode: Int, refer: String)
