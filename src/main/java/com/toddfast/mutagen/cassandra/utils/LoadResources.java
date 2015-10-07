package com.toddfast.mutagen.cassandra.utils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.CassandraMutagen;

public class LoadResources {

    private static Logger log = LoggerFactory.getLogger(LoadResources.class);

    /**
     * Sorts by root file name, ignoring path and file extension
     * 
     */
    private static final Comparator<String> COMPARATOR =
            new Comparator<String>() {
                @Override
                public int compare(final String path1, final String path2) {
                    try {
                        int index1 = path1.lastIndexOf("/");
                        int index2 = path2.lastIndexOf("/");

                        String file1;
                        if (index1 != -1) {
                            file1 = path1.substring(index1 + 1);
                        }
                        else {
                            file1 = path1;
                        }

                        String file2;
                        if (index2 != -1) {
                            file2 = path2.substring(index2 + 1);
                        }
                        else {
                            file2 = path2;
                        }

                        index1 = file1.lastIndexOf(".");
                        index2 = file2.lastIndexOf(".");

                        if (index1 > 1) {
                            file1 = file1.substring(0, index1);
                        }

                        if (index2 > 1) {
                            file2 = file2.substring(0, index2);
                        }

                        return file1.compareTo(file2);
                    }
                    catch (StringIndexOutOfBoundsException e) {
                        throw new StringIndexOutOfBoundsException(e.getMessage() +
                                " (path1: \"" + path1 +
                                "\", path2: \"" + path2 + "\")");
                    }
                }
            };

    // Methods
    /**
     * private constructor to prevent instantiation.
     */
    private LoadResources() {
    }

    /**
     * Retrives all the scripts under the indicated resource path.
     * Sort them according their datetime, save them.
     * 
     * @param mutagen
     *            - The cassandraMutagen instance that executes mutations.
     * @param rootResourcePath
     *            - resource path.
     * @param resourceScannerPatternFilter
     *            - Pattern filter.
     * @return List of resources founded.
     * @throws IOException
     */
    public static List<String> loadResources(CassandraMutagen mutagen, String rootResourcePath, String resourceScannerPatternFilter) throws IOException {
        List<String> resources = new ArrayList<>();

        try {
            List<String> discoveredResources =
                    ResourceScanner.getInstance().getResources(
                            rootResourcePath, Pattern.compile(resourceScannerPatternFilter),
                            mutagen.getClass().getClassLoader());
            // Make sure we found some resources
            if (discoveredResources.isEmpty()) {
                throw new IllegalArgumentException("Could not find resources " +
                        "on path \"" + rootResourcePath + "\"");
            }
            // Sort the resources with the comparator
            Collections.sort(discoveredResources, COMPARATOR);

            // Clean the resources

            for (String resource : discoveredResources) {
                log.info("Found mutation resource \"{}\"", resource);

                if (resource.endsWith(".class") || resource.endsWith(".java")) {
                    // Remove the file path
                    resource = relativize(resource, rootResourcePath);
                    if (resource.contains("$")) {
                        // skip inner classes
                        continue;
                    }
                }
                resources.add(resource);
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Could not find resources on " +
                    "path \"" + rootResourcePath + "\"", e);
        }
        return resources;
    }

    /**
     * Returns a new path that points to the same location as the first parameter, but starts with the second parameter.
     * If the second parameter is not contained in the first, returns an empty string.
     *
     * This is equivalent to pathToRelativize.substring(pathToRelativize.indexOf(root)), except
     * that it will correctly handle the case where the two paths don't use the same separator.
     *
     * Example: x/y/z/a/b/c/d/e, a/b/c -> a/b/c/d/e
     */
    private static String relativize(String pathToRelativize, String root) {
        Path rootPath = Paths.get(root);
        Path result = Paths.get(pathToRelativize);
        while (result.getNameCount() > 1 && !result.startsWith(rootPath)) {
            result = result.subpath(1, result.getNameCount());
        }
        return result.toString();
    }
}
