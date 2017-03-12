package net.boreeas.lookingglass.backend

import com.mchange.v2.c3p0.ComboPooledDataSource
import net.boreeas.lookingglass.Main
import net.boreeas.reweave.PublicApiConnection
import net.boreeas.reweave.RequestException
import org.apache.cxf.interceptor.Fault
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import java.time.OffsetDateTime
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

/**
 * @author Malte Schütze
 */
class MatchHistoryScraper(
        seedIds: List<String>,
        private val apiProvider: ApiProvider,
        private val dataSource: ComboPooledDataSource
) : Runnable {
    companion object {
        private val QUEUE_MAX_REFILL_SIZE = 1024 * 1024
        private val MAX_CONCURRENCY = 100
    }

    private var shardboundApi: PublicApiConnection = apiProvider.getApi()

    private val idsInQueue = HashSet<String>(seedIds) // Fast checking
    private val ids = ArrayDeque<String>(seedIds) // Ordering

    private val concurrencyAvailability = ArrayBlockingQueue<Any>(MAX_CONCURRENCY)
    private val MARKER = Object()

    private val idsInThreads = ArrayDeque<String>()
    private val threads = ArrayDeque<Future<Any>>()

    private var mainThreadDbConn = dataSource.getDbConnection()

    init {
        dbRetry { mainThreadDbConn.createSchema() }
        for (i in 0..MAX_CONCURRENCY - 1) {
            concurrencyAvailability.put(MARKER)
        }
    }

    private fun <T> apiRetry(block: () -> T): T {
        while (true) {
            try {
                return block()
            } catch (ex: RequestException) {
                System.err.println("${ex.message}, retrying in a bit")
                Thread.sleep(1000)
            } catch (ex: Fault) {
                System.err.println("${ex.message}, retrying in a bit")
                Thread.sleep(1000)
            }
        }
    }

    private fun <T> dbRetry(block: () -> T): T {
        while (true) {
            try {
                return block()
            } catch (ex: SQLException) {
                ex.printStackTrace()
                System.err.println("Probably a connection error, opening a new connection")
                mainThreadDbConn.close()
                mainThreadDbConn = dataSource.getDbConnection()
            }
        }
    }

    private fun <T> dbRetry(conn: DBConnection, block: DBConnection.() -> T): T {
        var conn = conn
        while (true) {
            try {
                return block(conn)
            } catch (ex: SQLException) {
                ex.printStackTrace()
                System.err.println("Probably a connection error, opening a new connection")
                conn.close()
                conn = dataSource.getDbConnection()
            }
        }
    }

    override fun run() {
        var cycleCount = 0

        println("Starting...")
        if (ids.isEmpty()) {
            refillQueue(cycleCount++)
        }

        while (true) {
            workOnQueue()
            analyzeGames()
            refillQueue(cycleCount++)

            if (cycleCount % 8 == 0) {
                println("[${Instant.now()}] Did a full cycle, sleeping a bit then logging back in")
                Thread.sleep(60 * 1000)
                println("Relogin")
                shardboundApi = apiRetry { apiProvider.getApi() }
            }
        }
    }

    private fun finishQueue() {
        println("Estimating ${ids.size} players to update")
        var c = 0
        while (ids.isNotEmpty()) {
            c++
            if (c % Main.DEBUG_RATE == 0) println("... ${ids.size} players remaining")

            val next = ids.remove()
            idsInQueue.remove(next)
            updatePlayer(next)
        }
    }


    private fun refillQueue(cycleCount: Int) {
        /*
         * Update players who played within the last
         * - day        =>  every update
         * - week       =>  every other update
         * - month      =>  every 4th update
         * - ever       =>  every 8nd update
         */
        val latestGameAfter = if (cycleCount % 8 == 0) {
            DBConnection.MIN_DATETIME
        } else if (cycleCount % 4 == 0) {
            OffsetDateTime.now().minusMonths(1)
        } else if (cycleCount % 2 == 0) {
            OffsetDateTime.now().minusWeeks(1)
        } else {
            OffsetDateTime.now().minusDays(1)
        }

        val longestUnupdatedPlayers = dbRetry {
            mainThreadDbConn.getLongestUnupdatedPlayers(QUEUE_MAX_REFILL_SIZE, latestGameAfter)
        }
        println("Refilling queue with ${longestUnupdatedPlayers.count()} ids")
        for (id in longestUnupdatedPlayers) {
            idsInQueue.add(id)
            ids.offer(id)
        }
    }

    private fun enqueue(id: String) {
        if (!idsInQueue.contains(id)) {
            idsInQueue.add(id)
            ids.offer(id)
        }
    }

    private fun updatePlayer(id: String) {
        val playerExists = dbRetry { mainThreadDbConn.playerExists(id) }
        if (!playerExists) {
            val userData = apiRetry { shardboundApi.user.retrieve(id).get() }
            dbRetry { mainThreadDbConn.createPlayer(userData) }
        }

        concurrencyAvailability.take()
        idsInThreads.add(id)
        threads.add(shardboundApi.user.withMatchHistory(id) {
            val dbConn = dataSource.getDbConnection()

            val history = it.filter { it.opponentId != null }
            history.filterNot { dbRetry(dbConn) { this.playerExists(it.opponentId!!) } }.forEach { enqueue(it.opponentId!!) }
            dbRetry(dbConn) { this.insertGames(history, id) }

            concurrencyAvailability.add(MARKER)
            dbConn.close()
        })
    }

    private fun workOnQueue() {
        do {
            finishQueue()
            println("Update finished, joining remaining games")

            while (threads.isNotEmpty()) {
                val id = idsInThreads.remove()
                try {
                    threads.remove().get()
                } catch (ex: ExecutionException) {
                    System.err.println("$id: Failed (${ex.message}), retrying")
                    ids.add(id)
                    idsInQueue.add(id)
                }
            }
        } while (ids.isNotEmpty())
    }

    private fun analyzeGames() {
        println("Analyzing games")
        var gameDataStream = dataSource.connection
        val stmt = gameDataStream.prepareStatement("""SELECT
                game.game_id AS game_id,
                pl1.end_result AS end_result,
                player1.player_id AS player_a,
                player2.player_id AS player_b,
                game.start_date
                        FROM Games game
                , Players player1 INNER JOIN PlayerPlayedGame pl1 USING (player_id)
                , Players player2 INNER JOIN PlayerPlayedGame pl2 USING (player_id)
                WHERE player1.player_id < player2.player_id
                AND pl1.game_id = game.game_id AND pl2.game_id = game.game_id
                AND pl1.elo_diff IS NULL AND pl2.elo_diff IS NULL
                AND pl1.end_result != 'Coop Win' AND pl1.end_result != 'Coop Loss'
                ORDER BY start_date ASC
                """)

        var result: ResultSet
        while (true) {
            try {
                result = stmt.executeQuery()
                break
            } catch (ex: SQLException) {
                System.err.println("Error retrieving unupdated games, retrying")
                gameDataStream.close()
                gameDataStream = dataSource.connection
            }
        }
        var counter = 0
        print("0 games updates")
        while (result.next()) {
            updateElo(
                    result.getString("game_id"),
                    result.getString("end_result"),
                    result.getString("player_a"),
                    result.getString("player_b")
            )

            counter++
            if (counter % Main.DEBUG_RATE == 0) {
                print("\r$counter games updated")
                System.out.flush()
            }
        }
        println("\r$counter games updated")
        gameDataStream.close()
    }

    private fun updateElo(game: String, end_result: String, playerA: String, playerB: String) {
        val playerAElo = dbRetry { mainThreadDbConn.getElo(playerA) }
        val playerBElo = dbRetry { mainThreadDbConn.getElo(playerB) }

        // How much the opponent is better than the player, where each 400 points are roughly
        // a 10x difference in skill
        val normalizedRatingDifference = (playerBElo - playerAElo) / 400.0

        // Assume that skill is modelled along a logistic curve
        val playerWinExpectancy = 1.0 / (1 + Math.pow(10.0, normalizedRatingDifference))
        val playerScore = when (end_result) {
            "Loss" -> 0.0
            "Win" -> 1.0
            "Draw" -> 0.5
            else -> throw RuntimeException("Trying to rate coop game ($end_result)")
        }

        var playerScoreDiff = 40 * (playerScore - playerWinExpectancy)
        if (playerScoreDiff >= 0 && playerScoreDiff < 1) {
            playerScoreDiff = 1.0
        } else if (playerScoreDiff < 0 && playerScoreDiff > -1) {
            playerScoreDiff = -1.0
        }


        dbRetry {
            mainThreadDbConn.updateEloValues(game, playerA, playerScoreDiff.toInt(), playerB, -playerScoreDiff.toInt())
        }
    }
}