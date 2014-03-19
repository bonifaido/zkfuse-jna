package zkfuse;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.StructFuseFileInfo;
import net.fusejna.StructStat;
import net.fusejna.types.TypeMode;
import net.fusejna.util.FuseFilesystemAdapterFull;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.data.Stat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;


public class ZookeeperFilesystem extends FuseFilesystemAdapterFull {

    private final CuratorFramework curator;

    public ZookeeperFilesystem(String connectString) {
        this.curator = CuratorFrameworkFactory.newClient(connectString, new ExponentialBackoffRetry(1000, 5));
    }

    @Override
    public void init() {
        curator.start();
    }

    @Override
    public void destroy() {
        curator.close();
    }

    @Override
    public int getattr(String path, StructStat.StatWrapper fsStat) {
        try {
            Stat zkStat = curator.checkExists().forPath(path);
            if (zkStat.getNumChildren() > 0) {
                fsStat.mode(TypeMode.NodeType.DIRECTORY.getBits() | 0777);
            } else {
                fsStat.mode(TypeMode.NodeType.FILE.getBits() | 0666);
            }
            fsStat.size(zkStat.getDataLength());
            fsStat.ctime(zkStat.getCtime() / 1000);
            fsStat.mtime(zkStat.getMtime() / 1000);
            return 0;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    public int mkdir(String path, TypeMode.ModeWrapper mode) {
        return createZNode(path);
    }

    @Override
    public int create(String path, TypeMode.ModeWrapper mode, StructFuseFileInfo.FileInfoWrapper info) {
        return createZNode(path);
    }

    private int createZNode(String path) {
        try {
            if (curator.checkExists().forPath(path) != null) {
                return -ErrorCodes.EEXIST();
            }
            curator.create().forPath(path);
            return 0;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    public int readdir(String path, DirectoryFiller filler) {
        try {
            List<String> children = curator.getChildren().forPath(path);
            filler.add(children);
            return 0;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    public int rename(String path, String newName) {
        throw new UnsupportedOperationException("rename");
    }

    @Override
    public int read(String path, ByteBuffer buffer, long size, long offset, StructFuseFileInfo.FileInfoWrapper info) {
        try {
            byte[] bytes = curator.getData().forPath(path);
            buffer.put(bytes);
            return bytes.length;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override // TODO fix
    public int write(String path, ByteBuffer buf, long bufSize, long writeOffset, StructFuseFileInfo.FileInfoWrapper wrapper) {
        try {
            byte[] bytes = curator.getData().forPath(path);
            byte[] newBytes = Arrays.copyOf(bytes, (int) (writeOffset + bufSize));
            buf.get(newBytes, (int) writeOffset, (int) bufSize);
            curator.setData().forPath(path, newBytes);
            return (int) bufSize;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    public int truncate(String path, long offset) {
        try {
            byte[] bytes = curator.getData().forPath(path);
            byte[] newBytes = Arrays.copyOf(bytes, (int) offset);
            curator.setData().forPath(path, newBytes);
            return 0;
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }
    }

    @Override
    public int rmdir(String path) {
        return unlink(path);
    }

    @Override
    public int unlink(String path) {
        try {
            curator.delete().forPath(path);
            return 0;
        } catch (Exception e) {
            return ErrorCodes.ENOENT();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: zkfuse [connectString] [mountPoint]");
            System.exit(1);
        }
        new ZookeeperFilesystem(args[0]).log(true).mount(args[1]);
    }
}
