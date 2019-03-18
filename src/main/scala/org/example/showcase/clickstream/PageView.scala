package org.example.showcase.clickstream

case class PageViewMessage(payload: PageView)

case class PageView(viewtime: Long, userid: String, pageid: String)
