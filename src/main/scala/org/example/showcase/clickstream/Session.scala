package org.example.showcase.clickstream

case class Session(user_id: String, duration: Long, page_count: Long, avg_page_time: Double)
