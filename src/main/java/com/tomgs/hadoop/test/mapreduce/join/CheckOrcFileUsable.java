package com.tomgs.hadoop.test.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.orc.OrcFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 检查orc文件是否可用，即文件是否为正确的orc文件
 *
 * @author tangzhongyuan
 * @create 2018-11-06 11:55
 **/
public class CheckOrcFileUsable {

    private static Logger logger = LoggerFactory.getLogger(CheckOrcFileUsable.class);

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        String hdfsRootPath = args[0];
        String hdfsUser = args[1];
        String orcDir = args[2];

        logger.info("start check dir [{}]...", orcDir);

        FileSystem fileSystem;
        Configuration conf = new Configuration();
        Configuration configuration = new Configuration();
        if ("localhost".equalsIgnoreCase(hdfsRootPath)) {
            fileSystem = FileSystem.get(configuration);
        } else {
            configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fileSystem = FileSystem.get(new URI(hdfsRootPath), configuration, hdfsUser);
        }

        Path orcDirPath = new Path(orcDir);
        if (!fileSystem.exists(orcDirPath)) {
            logger.info("path [{}] is not exists...", orcDirPath.toString());
            return ;
        }

        RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(orcDirPath, false);
        while (remoteIterator.hasNext()) {
            LocatedFileStatus next = remoteIterator.next();
            Path path = next.getPath();
            try {
                //如果文件错误，则reader时会出错
                OrcFile.createReader(path, OrcFile.readerOptions(conf).filesystem(fileSystem));
            } catch (Exception e) {
                logger.error("error file:[{}]", path.toString());
            }
        }
    }
}