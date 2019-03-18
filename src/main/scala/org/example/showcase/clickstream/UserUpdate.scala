package org.example.showcase.clickstream

case class UserUpdateMessage(payload: UserUpdate)

case class UserUpdate(registertime: Long, userid: String, regionid: String, gender: String)
