package com.example.runner.codeindex;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PathFilter {
    private final Path root;
    private final List<PathMatcher> includeMatchers;
    private final List<PathMatcher> excludeMatchers;

    private static final List<String> DEFAULT_EXCLUDES
            = Arrays.asList("target/**", "build/**", ".idea/**", "generated/**", "**/test/**", "**/tests/**", "**/it/**");

    public PathFilter(Path root, List<String> includes, List<String> excludes) {
        this.root = root.toAbsolutePath().normalize();
        FileSystem fs = FileSystems.getDefault();
        List<String> ex = new ArrayList<>(DEFAULT_EXCLUDES);
        if (excludes != null) ex.addAll(excludes);

        this.includeMatchers = compileMatchers(fs, includes);
        this.excludeMatchers = compileMatchers(fs, ex);
    }

    public boolean accept(Path p) {
        Path rel = safeRel(root, p);
        for (PathMatcher m : excludeMatchers) {
            if (m.matches(rel)) return false;
        }
        if (!includeMatchers.isEmpty()) {
            for (PathMatcher m : includeMatchers) {
                if (m.matches(rel)) return true;
            }
            return false;
        }
        return true;
    }

    private static List<PathMatcher> compileMatchers(FileSystem fs, List<String> globs) {
        List<PathMatcher> ms = new ArrayList<>();
        if (globs != null) {
            for (String g : globs) {
                if (g == null || g.trim().isEmpty()) continue;
                String pat = g.startsWith("glob:") || g.startsWith("regex:") ? g : "glob:" + g;
                ms.add(fs.getPathMatcher(pat));
            }
        }
        return ms;
    }

    private static Path safeRel(Path root, Path p) {
        try {
            return root.relativize(p.toAbsolutePath().normalize());
        } catch (Exception ignore) {
            // 回退到文件名（防止跨盘符等）
            return p.getFileName();
        }
    }
}