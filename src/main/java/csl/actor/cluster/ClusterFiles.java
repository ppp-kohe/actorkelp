package csl.actor.cluster;

import csl.actor.util.ConfigBase;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class ClusterFiles {
    protected String appName;
    protected ClusterCommands.ClusterUnit<?> unit;

    public ClusterFiles(String appName, ClusterCommands.ClusterUnit<?> unit) {
        this.appName = appName;
        this.unit = unit;
    }

    public void deployFiles() throws Exception {
        Path appDir = Paths.get(appName);
        if (Files.exists(appDir)) {
            throw new RuntimeException("failed creation: " + appDir);
        }

        Path jarDir = appDir.resolve("jars");
        createDirectories(jarDir);

        String path = System.getProperty("java.class.path", "");
        String[] items = path.split(Pattern.quote(File.pathSeparator));
        List<String> pathList = new ArrayList<>();
        for (String pathItem : items) {
            Path filePath = Paths.get(pathItem);
            if (Files.isDirectory(filePath)) { //creates jar and copies it
                deployFilesDirToJar(filePath, jarDir, pathList);
            } else if (Files.exists(filePath)){ //copy jar
                deployFilesCopyFile(filePath, jarDir, pathList);
            }
        }

        unit.setClassPathList(pathList);

        deployFilesConfig(appDir);
    }

    protected void deployFilesConfig(Path appDir) throws Exception {
        ClusterCommands.CommandBlock block = unit.getBlock();
        if (block != null) {
            while (block instanceof ClusterCommands.CommandBlockNamed) {
                ClusterCommands.CommandBlock p = ((ClusterCommands.CommandBlockNamed) block).getParent();
                if (p == null) {
                    break;
                } else {
                    block = p;
                }
            }
            StringBuilder buf = new StringBuilder();
            block.write(l -> buf.append(l).append("\n"));
            String data = buf.toString();
            deployFilesTextFile(appDir.resolve(ClusterDeployment.NON_DRIVER_FILE_CONF), data);
        }

        ConfigDeployment depConf = unit.getDeploymentConfig();
        if (depConf != null) {
            deployFilesTextFile(appDir.resolve("config-deploy.txt"), depConf.toStringConfig());
        }

        ConfigBase appConf = unit.getAppConfig();
        if (appConf != null) {
            deployFilesTextFile(appDir.resolve("config-app.txt"), appConf.toStringConfig());
        }
    }

    protected void deployFilesTextFile(Path path, String data) throws Exception {
        String header = unit.getDeploymentConfig().getAddress();
        unit.log("%s deployFiles: %s\n text-file -> %s", appName, header, path);
        writeString(path, data);
    }

    protected void deployFilesDirToJar(Path filePath, Path jarDir, List<String> pathList) throws Exception {
        String header = unit.getDeploymentConfig().getAddress();
        int count = pathList.size();
        String fileName = String.format("%s-%03d.jar", filePath.getFileName(), count);
        Path jarPath = jarDir.resolve(fileName);
        pathList.add(jarPath.toString());

        unit.log("%s deployFiles: %s\n      %s\n  ->  %s", appName, header, filePath, jarPath);
        try (Stream<Path> ps = Files.walk(filePath);
             JarOutputStream out = new JarOutputStream(
                     new BufferedOutputStream(openForWrite(jarPath)))) {
            ps.forEachOrdered(p -> writeToJar(filePath, p, out));
        }
    }

    public void writeToJar(Path startDir, Path path, JarOutputStream out) {
        try {
            String p = startDir.relativize(path).toString()
                    .replaceAll(Pattern.quote(File.separator), "/");
            boolean dir = Files.isDirectory(path);
            if (dir && !p.endsWith("/")) {
                p += "/";
            }
            JarEntry e = new JarEntry(p);
            try {
                FileTime time = Files.getLastModifiedTime(path);
                e.setLastModifiedTime(time);
            } catch (Exception ex) {
                //
            }
            out.putNextEntry(e);
            try {
                if (!dir) {
                    Files.copy(path, out);
                }
            } finally {
                out.closeEntry();
            }
        } catch (Exception ex) {
            unit.log("error writeToJar: %s :  %s",path, ex);
        }
    }

    protected void deployFilesCopyFile(Path filePath, Path jarDir, List<String> pathList) throws Exception {
        String header = unit.getDeploymentConfig().getAddress();
        int count = pathList.size();
        String fileName = filePath.getFileName().toString();
        Path jarPath = jarDir.resolve(fileName);
        unit.log("%s deployFiles: %s\n      %s\n  ->  %s", appName, header, filePath, jarPath);
        if (pathList.contains(jarPath.toString())) {
            int i = fileName.lastIndexOf(".");
            if (i > 0) {
                String pre = fileName.substring(0, i);
                String pos = fileName.substring(i);
                jarPath = jarDir.resolve(String.format("%s-%03d%s", pre, count, pos));
            } else {
                jarPath = jarDir.resolve(String.format("%s-%03d", fileName, count));
            }
        }
        copy(filePath, jarPath);
        pathList.add(jarPath.toString());
    }

    protected void createDirectories(Path dir) throws Exception {
        Files.createDirectories(getTargetPath(dir));
    }

    protected void writeString(Path path, String data) throws Exception {
        Files.writeString(getTargetPath(path), data);
    }

    protected void copy(Path localPath, Path targetPath) throws Exception {
        Files.copy(localPath, getTargetPath(targetPath));
    }

    protected OutputStream openForWrite(Path targetPath) throws Exception {
        return new FileOutputStream(getTargetPath(targetPath).toFile());
    }

    protected Path getTargetPath(Path path) {
        return Paths.get(unit.getDeploymentConfig().baseDir).resolve(path);
    }

    public static class ClusterFilesSsh extends ClusterFiles {
        protected Path directory;

        public ClusterFilesSsh(String appName, ClusterCommands.ClusterUnit<?> unit) throws Exception {
            super(appName, unit);
            directory = Files.createTempDirectory(getClass().getName().replaceAll("[$.]", "-"));
        }

        @Override
        protected Path getTargetPath(Path path) {
            return directory.resolve(path);
        }

        @Override
        public void deployFiles() throws Exception {
            super.deployFiles();

            List<String> inTar = Arrays.asList("tar", "cf", "-", ".");
            ProcessBuilder inBuilder = new ProcessBuilder().command(inTar)
                    .directory(directory.toFile())
                    .redirectError(ProcessBuilder.Redirect.INHERIT);

            Process inProc = inBuilder.start();

            List<String> outTar = ClusterDeployment.sshCommand(unit,
                        String.format("cd %s && tar x -C .", unit.getDeploymentConfig().baseDir));

            ProcessBuilder outBuilder = new ProcessBuilder().command(outTar)
                    .directory(new File("."))
                    .redirectError(ProcessBuilder.Redirect.INHERIT);

            Process outProc = outBuilder.start();

            unit.log("%s: ssh deploy %s\n   local: %s\n  remote: %s", appName, directory, inTar, outTar);

            transfer(inProc.getInputStream(), outProc.getOutputStream());

            inProc.waitFor();
            outProc.waitFor();
        }

        private void transfer(InputStream in, OutputStream out) throws Exception {
            in.transferTo(out);
            in.close();
            out.close();
        }
    }
}
