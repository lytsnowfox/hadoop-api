package personal.liyitong.hadoop.service;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import personal.liyitong.hadoop.config.HdfsConfig;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service(value = "hdfsServiceImpl")
public class HdfsServiceImpl implements HdfsService {

    @Autowired
    private HdfsConfig hdfsConfig;

    @Override
    public boolean mkdir(String path) {
        //目标路径
        Path newPath = new Path(path);
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            //创建空文件夹
            fs.mkdirs(newPath);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean createFile(String path, MultipartFile file) {
        String fileName = file.getOriginalFilename();
        //上传默认当前目录，后面的自动拼接文件的目录
        Path newPath = new Path(path + "/" + fileName);
        //打开输出流
        try (FileSystem fs = hdfsConfig.getFileSystem();
             FSDataOutputStream outputStream = fs.create(newPath)) {
            outputStream.write(file.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean uploadFile(String path, String uploadPath) {
        Path clientPath = new Path(path);
        Path serverPath = new Path(uploadPath);
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            fs.copyFromLocalFile(false, clientPath, serverPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public File downloadFile(String path, String downloadPath) {
        Path clientPath = new Path(path);
        Path serverPath = new Path(downloadPath);
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            fs.copyToLocalFile(false, clientPath, serverPath);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        File file = new File(downloadPath);
        return file;
    }

    @Override
    public boolean deleteFile(String path) {
        Path newPath = new Path(path);
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            // 如果目标是文件夹则递归删除
            fs.delete(newPath, true);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean copyFile(String sourcePath, String targetPath) {
        Path oldPath = new Path(sourcePath);
        Path newPath = new Path(targetPath);
        try (FileSystem fs = hdfsConfig.getFileSystem();
             FSDataInputStream inputStream = fs.open(oldPath);
             FSDataOutputStream outputStream = fs.create(newPath)) {
            IOUtils.copyBytes(inputStream, outputStream, 1024 * 1024 * 64, false);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public boolean renameFile(String oldName, String newName) {
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            fs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    @Override
    public void readFile(String path) {
        Path newPath = new Path(path);

        try (FileSystem fs = hdfsConfig.getFileSystem();
             InputStream in = fs.open(newPath)) {
            IOUtils.copyBytes(in, System.out, 4096);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Map<String, Object>> readPathInfo(String path) {
        Path newPath = new Path(path);
        FileStatus[] statusLists = new FileStatus[0];
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            statusLists = fs.listStatus(newPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Map<String, Object>> list = new ArrayList<>();
        if (null != statusLists && statusLists.length > 0) {
            for (FileStatus fileStatus : statusLists) {
                Map<String, Object> map = new HashMap<>();
                map.put("filePath", fileStatus.getPath());
                map.put("fileStatus", fileStatus.toString());
                list.add(map);
            }
            return list;
        } else {
            return null;
        }
    }

    @Override
    public void listFile(String path) {
        Path newPath = new Path(path);
        //递归找到所有文件
        RemoteIterator<LocatedFileStatus> filesList = null;
        try (FileSystem fs = hdfsConfig.getFileSystem()) {
            filesList = fs.listFiles(newPath, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<Map<String, String>> returnList = new ArrayList<>();
        while (true) {
            try {
                if (!filesList.hasNext()) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            LocatedFileStatus next = null;
            try {
                next = filesList.next();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String fileName = next.getPath().getName();
            Path filePath = next.getPath();
            Map<String, String> map = new HashMap<>();
            map.put("filName", fileName);
            map.put("filePath", filePath.toString());
            returnList.add(map);
        }
    }
}
