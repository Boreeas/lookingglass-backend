package net.boreeas.lookingglass

import com.github.bucket4j.Buckets
import com.mchange.v2.c3p0.ComboPooledDataSource
import net.boreeas.lookingglass.backend.LoginApiProvider
import net.boreeas.lookingglass.backend.MatchHistoryScraper
import java.io.FileInputStream
import java.util.*
import java.util.concurrent.TimeUnit

/**
 * @author Malte Schütze
 */
object Main {
    var DEBUG = false
    var DEBUG_RATE = 1000;

    @JvmStatic fun main(args: Array<String>) {
        val prop = Properties()
        prop.load(FileInputStream("config.secret"))
        DEBUG = prop.getProperty("debug", "false").toBoolean()
        DEBUG_RATE = prop.getProperty("debug.rate", "1000").toInt()

        val ratelimitBurst = prop.getProperty("ratelimitBurst").toLong()
        val rateLimitPerSecond = prop.getProperty("ratelimitPerSecond").toLong()
        val rateLimitInterval = 1000 * ratelimitBurst / rateLimitPerSecond
        val bucket = Buckets
                .withMillisTimePrecision()
                .withLimitedBandwidth(ratelimitBurst, TimeUnit.MILLISECONDS, rateLimitInterval)
        val provider = LoginApiProvider(prop.getProperty("username"), prop.getProperty("password"), bucket)

        val dataSource = ComboPooledDataSource()
        dataSource.jdbcUrl = "jdbc:postgresql://localhost:5432/" + prop.getProperty("dbname")
        dataSource.user = prop.getProperty("dbuser")
        dataSource.password = prop.getProperty("dbpass")
        dataSource.maxPoolSize = 15
        val scraper = MatchHistoryScraper(args.asList(), provider, dataSource)

        scraper.run()
    }
}
