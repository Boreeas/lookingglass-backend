package net.boreeas.lookingglass

class ResourceHolder : AutoCloseable {
    val resources = arrayListOf<AutoCloseable>()

    fun <T : AutoCloseable> T.autoClose(): T {
        resources.add(this)
        return this
    }

    override fun close() {
        resources.reversed().forEach { it.close() }
    }
}


fun <R> using(block: ResourceHolder.() -> R): R {
    val holder = ResourceHolder()
    try {
        return holder.block()
    } finally {
        holder.close()
    }
}