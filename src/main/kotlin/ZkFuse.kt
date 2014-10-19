package zkfuse

import net.fusejna.util.FuseFilesystemAdapterFull
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import net.fusejna.StructStat
import net.fusejna.types.TypeMode
import net.fusejna.ErrorCodes
import net.fusejna.StructFuseFileInfo
import net.fusejna.DirectoryFiller
import java.nio.ByteBuffer
import java.util.Arrays
import org.apache.curator.framework.recipes.cache.TreeCache
import org.apache.curator.framework.recipes.cache.TreeCacheListener
import org.apache.curator.framework.recipes.cache.TreeCacheEvent
import java.util.concurrent.CountDownLatch


public class ZkFuse(connectString: String) : FuseFilesystemAdapterFull() {

    val curator = CuratorFrameworkFactory.newClient(connectString, ExponentialBackoffRetry(1000, 5))
    val cache = TreeCache(curator, "/")

    override fun init() {
        curator.start()
        val latch = CountDownLatch(1)
        cache.getListenable().addListener(TreeCacheListener { _, e ->
            if (e.getType() == TreeCacheEvent.Type.INITIALIZED) latch.countDown()
        })
        cache.start()
        latch.await() // Wait until cache primes
    }

    override fun destroy() {
        cache.close()
        curator.close()
    }

    override fun getattr(path: String, fsStat: StructStat.StatWrapper): Int {
        val data = cache.getCurrentData(path)
        if (data == null) {
            return -ErrorCodes.ENOENT()
        }
        val zkStat = data.getStat()
        if (zkStat.getNumChildren() > 0) {
            fsStat.setMode(TypeMode.NodeType.DIRECTORY)
        } else {
            fsStat.setMode(TypeMode.NodeType.FILE)
        }
        fsStat.size(zkStat.getDataLength().toLong())
        fsStat.ctime(zkStat.getCtime() / 1000)
        fsStat.mtime(zkStat.getMtime() / 1000)
        return 0
    }

    override fun mkdir(path: String, mode: TypeMode.ModeWrapper): Int {
        return createZNode(path)
    }

    override fun create(path: String, mode: TypeMode.ModeWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int {
        return createZNode(path)
    }

    private fun createZNode(path: String): Int {
        if (cache.getCurrentData(path) != null) {
            return -ErrorCodes.EEXIST()
        }
        try {
            curator.create().forPath(path)
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
        return 0
    }

    override fun readdir(path: String, filler: DirectoryFiller): Int {
        val children = cache.getCurrentChildren(path)
        if (children == null) {
            return -ErrorCodes.ENOENT()
        }
        filler.add(children.keySet())
        return 0
    }

    override fun rename(path: String, newName: String): Int {
        throw UnsupportedOperationException("rename")
    }

    override fun read(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int {
        val data = cache.getCurrentData(path)
        if (data == null) {
            return -ErrorCodes.ENOENT()
        }
        buffer.put(data.getData())
        return data.getData().size
    }

    override // TODO fix
    fun write(path: String, buf: ByteBuffer, bufSize: Long, writeOffset: Long, wrapper: StructFuseFileInfo.FileInfoWrapper): Int {
        try {
            val bytes = curator.getData().forPath(path)
            val newBytes = Arrays.copyOf(bytes, (writeOffset + bufSize).toInt())
            buf.get(newBytes, writeOffset.toInt(), bufSize.toInt())
            curator.setData().forPath(path, newBytes)
            return bufSize.toInt()
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
    }

    override fun truncate(path: String, offset: Long): Int {
        try {
            val bytes = curator.getData().forPath(path)
            val newBytes = Arrays.copyOf(bytes, offset.toInt())
            curator.setData().forPath(path, newBytes)
            return 0
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
    }

    override fun rmdir(path: String): Int {
        return unlink(path)
    }

    override fun unlink(path: String): Int {
        try {
            curator.delete().forPath(path)
            return 0
        } catch (e: Exception) {
            return ErrorCodes.ENOENT()
        }
    }

    fun log() = log(true)
}

fun main(args: Array<String>) {
    if (args.size < 2) {
        System.err.println("usage: java -jar ZkFuse.jar [connectString] [mountPoint]")
        System.exit(1)
    }
    ZkFuse(args[0]).log().mount(args[1])
}
