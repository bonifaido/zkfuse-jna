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


public class ZkFuse(connectString: String) : FuseFilesystemAdapterFull() {

    val curator = CuratorFrameworkFactory.newClient(connectString, ExponentialBackoffRetry(1000, 5))

    override fun init() = curator.start()

    override fun destroy() = curator.close()

    override fun getattr(path: String, fsStat: StructStat.StatWrapper): Int {
        try {
            val zkStat = curator.checkExists().forPath(path)
            if (zkStat.getNumChildren() > 0) {
                fsStat.setMode(TypeMode.NodeType.DIRECTORY)
            } else {
                fsStat.setMode(TypeMode.NodeType.FILE)
            }
            fsStat.size(zkStat.getDataLength().toLong())
            fsStat.ctime(zkStat.getCtime() / 1000)
            fsStat.mtime(zkStat.getMtime() / 1000)
            return 0
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
    }

    override fun mkdir(path: String, mode: TypeMode.ModeWrapper): Int {
        return createZNode(path)
    }

    override fun create(path: String, mode: TypeMode.ModeWrapper, info: StructFuseFileInfo.FileInfoWrapper): Int {
        return createZNode(path)
    }

    private fun createZNode(path: String): Int {
        try {
            if (curator.checkExists().forPath(path) != null) {
                return -ErrorCodes.EEXIST()
            }
            curator.create().forPath(path)
            return 0
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
    }

    override fun readdir(path: String, filler: DirectoryFiller): Int {
        try {
            val children = curator.getChildren().forPath(path)
            filler.add(children)
            return 0
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
    }

    override fun rename(path: String, newName: String): Int {
        throw UnsupportedOperationException("rename")
    }

    override fun read(path: String, buffer: ByteBuffer, size: Long, offset: Long, info: StructFuseFileInfo.FileInfoWrapper): Int {
        try {
            val bytes = curator.getData().forPath(path)
            buffer.put(bytes)
            return bytes.size
        } catch (e: Exception) {
            return -ErrorCodes.ENOENT()
        }
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
}

fun main(args: Array<String>) {
    if (args.size < 2) {
        System.err.println("usage: java -jar ZkFuse.jar [connectString] [mountPoint]")
        System.exit(1)
    }
    ZkFuse(args[0]).mount(args[1])
}
