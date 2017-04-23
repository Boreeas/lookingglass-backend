package net.boreeas.lookingglass.backend

import com.codedisaster.steamworks.*
import com.github.bucket4j.BucketBuilder
import net.boreeas.lookingglass.Main
import net.boreeas.reweave.PublicApiConnection
import net.boreeas.reweave.ShardboundServer
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer

/**
 * @author Malte Schütze
 */
class SteamLoginApiProvider(private val bucketMaker: BucketBuilder?): ApiProvider {
    private val steamUser: SteamUser

    init {
        if (!File("steam_appid.txt").exists()) {
            FileOutputStream("steam_appid.txt").use { it.writer(Charsets.UTF_8).write("${Main.SHARDBOUND_STEAM_APPID}") }
        }

        SteamAPI.printDebugInfo(System.out)
        if (!SteamAPI.init()) {
            throw IllegalStateException("Unable to init steam api")
        }

        steamUser = SteamUser(object : SteamUserCallback {
            override fun onValidateAuthTicket(steamID: SteamID?, authSessionResponse: SteamAuth.AuthSessionResponse?, ownerSteamID: SteamID?) {
                println("[DBG] Validating auth ticket: steamId=$steamID authSessionResponse=$authSessionResponse ownerSteamId=$ownerSteamID")
            }

            override fun onMicroTxnAuthorization(appID: Int, orderID: Long, authorized: Boolean) {
                println("[DBG] Microtransaction authorized: appId=$appID orderId=$orderID authorized=$authorized")
            }

        })

    }

    override fun getApi(): PublicApiConnection {
        val buffer = ByteBuffer.allocateDirect(1024)
        val ticketResult = steamUser.getAuthSessionTicket(buffer, IntArray(1, { 1024 }))
        if (!ticketResult.isValid) {
            throw IllegalStateException("Unable to retrieve valid ticket")
        }
        val ticket = Main.hex(buffer)
        val conn = ShardboundServer().toAuthorizedConnection(ticket)
        if (bucketMaker != null) conn.bucket = bucketMaker.build()
        conn.retryOnError = true
        return conn
    }
}