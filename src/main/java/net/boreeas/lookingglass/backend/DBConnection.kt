package net.boreeas.lookingglass.backend

import net.boreeas.reweave.data.Game
import net.boreeas.reweave.data.GameEndCondition
import net.boreeas.reweave.data.User
import java.sql.Connection
import java.sql.SQLException
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*
import javax.sql.DataSource

/**
 * @author Malte Schütze
 */
class DBConnection(val connection: Connection) : AutoCloseable {

    companion object {
        // pgsql can't represent OffsetDateTime.MIN, so we craft our own
        val MIN_DATETIME = OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
        val MAX_DATETIME = OffsetDateTime.of(9001, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC)
    }

    override fun close() {
        connection.close()
    }

    fun createSchema() {
        val stmt = connection.createStatement()
        stmt.addBatch("""
                DO $$
                BEGIN
                  IF NOT EXISTS(SELECT 1
                                FROM pg_type
                                WHERE typname = 'gameendresult')
                  THEN
                    CREATE TYPE GameEndResult AS ENUM ('Win', 'Loss', 'Draw', 'Coop Win', 'Coop Loss');
                  END IF;
                END
                $$""")
        stmt.addBatch("""
                CREATE TABLE IF NOT EXISTS Players (
                    display_name          TEXT                     NOT NULL,
                    normalized_display_name TEXT                   NOT NULL,
                    player_id             TEXT PRIMARY KEY,
                    last_checked          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_DATE,
                    elo                   INT                      NOT NULL DEFAULT 0,
                    visibility_restricted BOOLEAN                  NOT NULL DEFAULT FALSE,
                    trigger_update        BOOLEAN                  NOT NULL DEFAULT FALSE
                )""")
        stmt.addBatch("""
                CREATE TABLE IF NOT EXISTS Games (
                    game_id    TEXT PRIMARY KEY,
                    start_date TIMESTAMP WITH TIME ZONE NOT NULL
                )""")
        stmt.addBatch("""
                CREATE TABLE IF NOT EXISTS PlayerPlayedGame (
                    game_id    TEXT REFERENCES Games ON DELETE CASCADE ON UPDATE CASCADE,
                    player_id  TEXT DEFAULT '#deleted#' REFERENCES Players ON DELETE SET DEFAULT ON UPDATE CASCADE,
                    end_result GameEndResult NOT NULL,
                    elo_diff   INT DEFAULT NULL,
                    PRIMARY KEY (game_id, player_id)
                )""")
        stmt.addBatch("""
                CREATE OR REPLACE RULE games_ignore_duplicate_inserts AS
                ON INSERT TO Games
                  WHERE (EXISTS(SELECT 1
                                FROM Games
                                WHERE Games.game_id = NEW.game_id)) DO INSTEAD NOTHING""")
        stmt.addBatch("""
                CREATE OR REPLACE RULE player_played_games_ignore_duplicate_inserts AS
                ON INSERT TO PlayerPlayedGame
                  WHERE (EXISTS(SELECT 1
                                FROM PlayerPlayedGame p
                                WHERE p.game_id = NEW.game_id AND p.player_id = NEW.player_id)) DO INSTEAD NOTHING""")
        stmt.addBatch("""
                CREATE OR REPLACE RULE players_ignore_duplicate_inserts AS
                ON INSERT TO Players
                  WHERE (EXISTS(SELECT 1
                                FROM Players p
                                WHERE p.player_id = NEW.player_id)) DO INSTEAD NOTHING""")
        stmt.addBatch("""
                DO $$
                BEGIN

                IF NOT EXISTS (
                    SELECT 1
                    FROM   pg_class c
                    JOIN   pg_namespace n ON n.oid = c.relnamespace
                    WHERE  c.relname = 'players_normalized_display_name_idx'
                    AND    n.nspname = 'public' -- 'public' by default
                    ) THEN

                    CREATE INDEX players_normalized_display_name_idx ON public.Players (normalized_display_name);
                END IF;

                END$$""")
        stmt.addBatch("""
                DO $$
                BEGIN

                IF NOT EXISTS (
                    SELECT 1
                    FROM   pg_class c
                    JOIN   pg_namespace n ON n.oid = c.relnamespace
                    WHERE  c.relname = 'playerplayedgame_player_id_idx'
                    AND    n.nspname = 'public' -- 'public' by default
                    ) THEN

                    CREATE INDEX playerplayedgame_player_id_idx ON public.PlayerPlayedGame (player_id);
                END IF;

                END$$""")
        stmt.addBatch("""
                DO $$
                BEGIN

                IF NOT EXISTS (
                    SELECT 1
                    FROM   pg_class c
                    JOIN   pg_namespace n ON n.oid = c.relnamespace
                    WHERE  c.relname = 'playerplayedgame_game_id_idx'
                    AND    n.nspname = 'public' -- 'public' by default
                    ) THEN

                    CREATE INDEX playerplayedgame_game_id_idx ON public.PlayerPlayedGame (game_id);
                END IF;

                END$$""")
        stmt.addBatch("SELECT set_limit(0.8)")
        stmt.executeBatch()

        if (!playerExists("#deleted#")) {
            val deletedPlayerInsert = connection.createStatement()
            deletedPlayerInsert.execute("INSERT INTO Players(display_name, normalized_display_name, player_id) VALUES ('User Hidden', 'user hidden', '#deleted#')")
        }
    }

    fun playerExists(playerId: String): Boolean {
        return retryAndCommit {
            val stmt = connection.prepareStatement("SELECT COUNT(*) FROM Players WHERE player_id = ?")
            stmt.setString(1, playerId)

            val result = stmt.executeQuery()
            result.next()

            result.getInt(1) > 0
        }
    }

    fun insertGames(games: Collection<Game>, for_player: String) {
        retryAndCommit {
            val insertGameStmt = connection.prepareStatement("INSERT INTO Games(game_id, start_date) VALUES (?, ?)")
            val playerToGameStmt = connection.prepareStatement(
                    "INSERT INTO PlayerPlayedGame(game_id, player_id, end_result) VALUES (?, ?, ?::GameEndResult)"
            )

            for (game in games) {
                insertGameStmt.setString(1, game.gameId!!)
                insertGameStmt.setObject(2, game.startDate!!)
                insertGameStmt.addBatch()

                val gameResultString = when (game.adjustedEndCondition!!) {
                    GameEndCondition.WIN, GameEndCondition.WIN_CONCEDE -> "Win"
                    GameEndCondition.LOSS, GameEndCondition.LOSS_CONCEDE -> "Loss"
                    GameEndCondition.DRAW -> "Draw"
                    GameEndCondition.COOP_WIN -> "Coop Win"
                    GameEndCondition.COOP_LOSS -> "Coop Loss"
                }
                playerToGameStmt.setString(1, game.gameId!!)
                playerToGameStmt.setString(2, for_player)
                playerToGameStmt.setString(3, gameResultString)
                playerToGameStmt.addBatch()
            }

            insertGameStmt.executeBatch()
            playerToGameStmt.executeBatch()
        }
    }

    fun getElo(player: String): Int {
        return retryAndCommit {
            val stmt = connection.prepareStatement("SELECT elo FROM Players WHERE player_id = ?")
            stmt.setString(1, player)
            val result = stmt.executeQuery()

            if (!result.next()) 0 /* Mean/unrated score */ else result.getInt(1)
        }
    }

    fun createPlayer(userData: User) {
        retryAndCommit {
            val stmt = connection.prepareStatement("INSERT INTO Players(player_id, display_name, normalized_display_name, last_checked, elo) VALUES(?, ?, ?, ?, 0)")
            stmt.setString(1, userData.userId!!)
            stmt.setString(2, userData.displayName!!)
            stmt.setString(3, userData.displayName!!.toLowerCase())
            stmt.setObject(4, MIN_DATETIME)
            stmt.execute()
        }
    }

    fun getPlayersToUpdate(max: Int, lastGameAfter: OffsetDateTime): List<String> {
        return retryAndCommit {
            val stmt = connection.prepareStatement(
                    """SELECT p.player_id
                        FROM Players p
                        WHERE player_id != '#deleted#'
                              AND ((SELECT MAX(start_date)
                                    FROM Games
                                      INNER JOIN PlayerPlayedGame pl USING (game_id)
                                    WHERE p.player_id = pl.player_id) >= ?
                                   OR NOT EXISTS(SELECT *
                                                 FROM playerplayedgame pl2
                                                 WHERE pl2.player_id = p.player_id)
                                   OR p.trigger_update)
                        ORDER BY last_checked ASC
                        LIMIT ?;
                        """
            )
            stmt.setObject(1, lastGameAfter)
            stmt.setInt(2, max)

            val result = stmt.executeQuery()
            val list = ArrayList<String>()
            while (result.next()) {
                list.add(result.getString("player_id"))
            }

            list
        }
    }

    fun markPlayerUpdated(id: String) {
        retryAndCommit {
            val stmt = connection.prepareStatement("UPDATE Players SET trigger_update = FALSE, last_checked = ? WHERE player_id = ?")
            stmt.setObject(1, OffsetDateTime.now())
            stmt.setString(2, id)

            stmt.execute()
        }
    }

    fun updateEloValues(game: String, player1Id: String, player1EloGain: Int, player2Id: String, player2EloGain: Int) {
        retryAndCommit {
            val updatePlayerStmt = connection.prepareStatement("UPDATE Players SET elo = elo + ? WHERE player_id = ?")
            updatePlayerStmt.setInt(1, player1EloGain)
            updatePlayerStmt.setString(2, player1Id)
            updatePlayerStmt.execute()
            updatePlayerStmt.setInt(1, player2EloGain)
            updatePlayerStmt.setString(2, player2Id)
            updatePlayerStmt.execute()

            val updateGameStmt = connection.prepareStatement("UPDATE PlayerPlayedGame SET elo_diff = ? WHERE player_id = ? AND game_id = ?")
            updateGameStmt.setInt(1, player1EloGain)
            updateGameStmt.setString(2, player1Id)
            updateGameStmt.setString(3, game)
            updateGameStmt.execute()
            updateGameStmt.setInt(1, player2EloGain)
            updateGameStmt.setString(2, player2Id)
            updateGameStmt.setString(3, game)
            updateGameStmt.execute()
        }
    }

    fun <T> retryAndCommit(block: () -> T): T {

        while (true) {
            try {
                connection.autoCommit = false
                val result = block.invoke()
                connection.commit()
                return result
            } catch (ex: SQLException) {
                if (ex.message?.contains("I/O error") ?: false) {
                    throw ex // Can't handle I/O errors at this level
                }
                ex.printStackTrace()
                System.err.println("Execution of SQL statement failed, retrying")
                // Retry
            } finally {
                connection.autoCommit = true
            }
        }
    }

    fun getLatestGame(id: String): OffsetDateTime {
        return retryAndCommit {
            val stmt = connection.prepareStatement("SELECT MAX(start_date) FROM Games INNER JOIN playerplayedgame USING (game_id) WHERE player_id = ?")
            stmt.setString(1, id)

            val result = stmt.executeQuery()
            if (result.next() && result.getObject(1) != null) {
                result.getObject(1, OffsetDateTime::class.java)
            } else {
                MIN_DATETIME
            }
        }
    }
}


fun <T : DataSource> T.getDbConnection(): DBConnection {
    return DBConnection(this.connection)
}