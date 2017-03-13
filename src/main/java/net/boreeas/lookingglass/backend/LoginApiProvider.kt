package net.boreeas.lookingglass.backend

import com.github.bucket4j.BucketBuilder
import net.boreeas.reweave.PublicApiConnection
import net.boreeas.reweave.ShardboundServer

/**
 * @author Malte Schütze
 */
class LoginApiProvider(
        private val name: String,
        private val password: String,
        private val bucketMaker: BucketBuilder?): ApiProvider {

    override fun getApi(): PublicApiConnection {
        val conn = ShardboundServer().toAuthorizedConnection(name, password)
        if (bucketMaker != null) conn.bucket = bucketMaker.build()
        conn.retryOnError = true
        return conn
    }
}