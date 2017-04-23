package net.boreeas.lookingglass

import com.github.bucket4j.Buckets
import com.mchange.v2.c3p0.ComboPooledDataSource
import net.boreeas.lookingglass.backend.MatchHistoryScraper
import net.boreeas.lookingglass.backend.SteamLoginApiProvider
import java.io.FileInputStream
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.TimeUnit


/**
 * @author Malte Schütze
 */
object Main {
    var DEBUG = false
    var DEBUG_RATE = 1000
    var SHARDBOUND_STEAM_APPID = 586030

    @JvmStatic fun main(args: Array<String>) {

        val prop = Properties()
        prop.load(FileInputStream("config.secret"))
        DEBUG = prop.getProperty("debug", "false").toBoolean()
        DEBUG_RATE = prop.getProperty("debug.rate", "1000").toInt()

        val ratelimitBurst = prop.getProperty("ratelimitBurst").toLong()
        val rateLimitPerSecond = prop.getProperty("ratelimitPerSecond").toDouble()
        val rateLimitInterval = 1000 * ratelimitBurst / rateLimitPerSecond
        val bucket = Buckets
                .withMillisTimePrecision()
                .withLimitedBandwidth(ratelimitBurst, TimeUnit.MILLISECONDS, rateLimitInterval.toLong())
        val provider = SteamLoginApiProvider(bucket)

        val dataSource = ComboPooledDataSource()
        dataSource.jdbcUrl = "jdbc:postgresql://localhost:5432/" + prop.getProperty("dbname")
        dataSource.user = prop.getProperty("dbuser")
        dataSource.password = prop.getProperty("dbpass")
        dataSource.maxPoolSize = 15
        val scraper = MatchHistoryScraper(args.asList(), provider, dataSource)

        scraper.run()
    }

    fun hex(buffer: ByteBuffer): String {
        val end = buffer.limit()
        val builder = StringBuilder()
        (0..end-1)
                .map { idx -> buffer[idx] }
                .forEach { builder.append(String.format("%02X", it)) }

        buffer.rewind()

        return builder.toString()
    }
}
