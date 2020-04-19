package personal.liyitong.hadoop.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.List;
import java.util.Map;


public interface HdfsService {
    boolean mkdir(String path);
    boolean createFile(String path, MultipartFile file);
    boolean uploadFile(String path, String uploadPath);
    File downloadFile(String path, String downloadPath);
    boolean deleteFile(String path);
    boolean copyFile(String sourcePath, String targetPath);
    boolean renameFile(String oldName, String newName);
    void readFile(String path);
    List<Map<String, Object>> readPathInfo(String path);
    void listFile(String path);
}
