package net.boreeas.lookingglass.backend

import net.boreeas.reweave.PublicApiConnection

/**
 * @author Malte Schütze
 */
interface ApiProvider {
    fun getApi(): PublicApiConnection
}