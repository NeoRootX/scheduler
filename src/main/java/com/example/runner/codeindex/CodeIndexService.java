package com.example.runner.codeindex;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.NodeList;
import com.github.javaparser.ast.body.*;
import com.github.javaparser.ast.comments.JavadocComment;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.type.ClassOrInterfaceType;
import com.github.javaparser.javadoc.Javadoc;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 线程安全 & 并行解析
 * <p>
 * - 为每个线程使用独立的 JavaParser（ThreadLocal）。
 * - 使用 per-run CombinedTypeSolver（构造一次）并注入 ParserConfiguration。
 * - 并行遍历 javaFiles（parallelStream），每个文件在当前线程用自己的 JavaParser 解析。
 * - 收集到本地 list 后，用短时 synchronized 将结果写入共享 CSVPrinter（避免并发写冲突）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CodeIndexService {

    public void jobProcess(Path root, Path out, List<String> includes,
                             List<String> excludes,
                             List<String> classpathEntries) throws Exception {

        Objects.requireNonNull(root, "root required");
        Objects.requireNonNull(out, "output required");

        // 规范化并放到 final 变量，供 lambda 捕获使用
        final Path rootPath = root.toAbsolutePath().normalize();
        Files.createDirectories(out);

        final PathFilter filter = new PathFilter(rootPath, includes, excludes);
        final CombinedTypeSolver solver = buildSolver(rootPath, classpathEntries);
        final ParserConfiguration cfg = new ParserConfiguration()
                .setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_11)
                .setAttributeComments(false)
                .setDoNotAssignCommentsPrecedingEmptyLines(false)
                .setSymbolResolver(new JavaSymbolSolver(solver));
        final ThreadLocal<JavaParser> parserTl = ThreadLocal.withInitial(() -> new JavaParser(cfg));

        final List<Path> javaFiles = collectJavaFiles(rootPath, filter);
        log.info("indexProject: root={} javaFiles={}", rootPath, javaFiles.size());

        // 锁：各 CSV 一个独立锁，短时同步写入
        final Object classesLock = new Object();
        final Object methodsLock = new Object();
        final Object fieldsLock = new Object();
        final Object callsLock = new Object();

        // 统计解析文件数量
        final AtomicInteger parsedCounter = new AtomicInteger(0);

        try (CSVPrinter classesCsv = csv(out.resolve("classes.csv"), "package", "kind", "name", "qualified", "modifiers", "extends", "implements", "typeParams", "annotations", "deprecated", "javadoc", "file", "line");
             CSVPrinter methodsCsv = csv(out.resolve("methods.csv"), "class", "method", "signature", "returnType", "modifiers", "annotations", "parameters", "throws", "deprecated", "javadoc", "file", "line");
             CSVPrinter fieldsCsv = csv(out.resolve("fields.csv"), "class", "field", "type", "modifiers", "annotations", "deprecated", "javadoc", "file", "line");
             CSVPrinter callsCsv = csv(out.resolve("calls.csv"), "callerClass", "callerMethod", "calleeQualified", "calleeSignature", "file", "line")) {

            // 并行处理每个 java 文件
            javaFiles.parallelStream().forEach(f -> {
                // 本线程 parser
                JavaParser parser = parserTl.get();

                // 本地收集容器，减少在解析过程里的同步开销
                final List<Object[]> classesBuf = new ArrayList<>();
                final List<Object[]> methodsBuf = new ArrayList<>();
                final List<Object[]> fieldsBuf = new ArrayList<>();
                final List<Object[]> callsBuf = new ArrayList<>();

                String code = null;
                try {
                    code = safeReadAllChars(f);
                } catch (Exception e) {
                    log.warn("read fail {}, skip: {}", f, e.toString());
                    return;
                }

                CompilationUnit cu;
                try {
                    ParseResult<CompilationUnit> pr = parser.parse(code);
                    if (!pr.isSuccessful() || !pr.getResult().isPresent()) {
                        log.warn("Parse failed (problems): {} -> problems: {}", f, pr.getProblems());
                        return;
                    }
                    cu = pr.getResult().get();
                } catch (Throwable ex) {
                    log.warn("Parse failed for {} : {}", f, ex.toString());
                    return;
                }

                final String pkg = cu.getPackageDeclaration().map(p -> p.getNameAsString()).orElse("");
                final String fileRel = rootPath.relativize(f.toAbsolutePath().normalize()).toString();

                for (TypeDeclaration<?> td : cu.findAll(TypeDeclaration.class)) {
                    String kind = kindOf(td);
                    String simpleName = td.getNameAsString();
                    String qualified = pkg.isEmpty() ? simpleName : pkg + "." + simpleName;
                    String mods = td.getModifiers()
                            .stream()
                            .map(Object::toString)
                            .collect(Collectors.joining(" "));
                    String ext = extractExtends(td);
                    String impl = extractImplements(td);
                    String typeParams = (td instanceof ClassOrInterfaceDeclaration) ? ((ClassOrInterfaceDeclaration) td).getTypeParameters().toString() : "";
                    String anns = joinAnnotations(td.getAnnotations());
                    boolean deprecated = hasDeprecated(td.getAnnotations());
                    String jdoc = javadocSummary(td);

                    classesBuf.add(new Object[]{pkg, kind, simpleName, qualified, mods, ext, impl, typeParams, anns, deprecated, jdoc, fileRel, line(td)});

                    for (BodyDeclaration<?> m : td.getMembers()) {
                        if (m instanceof FieldDeclaration) {
                            FieldDeclaration fd = (FieldDeclaration) m;
                            String fmods = fd.getModifiers()
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining(" "));
                            String fans = joinAnnotations(fd.getAnnotations());
                            boolean fdep = hasDeprecated(fd.getAnnotations());
                            String fjdoc = javadocSummary(fd);
                            for (VariableDeclarator v : fd.getVariables()) {
                                fieldsBuf.add(new Object[]{qualified, v.getNameAsString(), v.getTypeAsString(), fmods, fans, fdep, fjdoc, fileRel, line(fd)});
                            }
                        }
                    }

                    for (BodyDeclaration<?> m : td.getMembers()) {
                        if (m instanceof MethodDeclaration) {
                            MethodDeclaration md = (MethodDeclaration) m;
                            String mname = md.getNameAsString();
                            String ret = md.getType().asString();
                            String mmods = md.getModifiers()
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining(" "));
                            String manns = joinAnnotations(md.getAnnotations());
                            boolean mdep = hasDeprecated(md.getAnnotations());
                            String params = md.getParameters()
                                    .stream()
                                    .map(p -> p.getTypeAsString() + " " + p.getNameAsString())
                                    .collect(Collectors.joining(", "));
                            String sig = mname + "(" + md.getParameters()
                                    .stream()
                                    .map(p -> p.getTypeAsString())
                                    .collect(Collectors.joining(",")) + ")";
                            String throwses = md.getThrownExceptions()
                                    .stream()
                                    .map(Object::toString)
                                    .collect(Collectors.joining(", "));
                            String mjdoc = javadocSummary(md);

                            methodsBuf.add(new Object[]{qualified, mname, sig, ret, mmods, manns, params, throwses, mdep, mjdoc, fileRel, line(md)});

                            for (MethodCallExpr call : md.findAll(MethodCallExpr.class)) {
                                String calleeQualified = "";
                                String calleeSignature = call.getNameAsString() + "(" + call.getArguments()
                                        .stream()
                                        .map(a -> "?")
                                        .collect(Collectors.joining(",")) + ")";
                                try {
                                    ResolvedMethodDeclaration r = call.resolve();
                                    calleeQualified = r.getQualifiedName();
                                    calleeSignature = r.getSignature();
                                } catch (Throwable ignore) {
                                }
                                callsBuf.add(new Object[]{qualified, sig, calleeQualified, calleeSignature, fileRel, line(call)});
                            }
                        }
                    }
                }

                // 批量写入：每种 CSV 都做短时同步，减少锁竞争
                if (!classesBuf.isEmpty()) {
                    synchronized (classesLock) {
                        for (Object[] row : classesBuf) write(classesCsv, row);
                    }
                }
                if (!fieldsBuf.isEmpty()) {
                    synchronized (fieldsLock) {
                        for (Object[] row : fieldsBuf) write(fieldsCsv, row);
                    }
                }
                if (!methodsBuf.isEmpty()) {
                    synchronized (methodsLock) {
                        for (Object[] row : methodsBuf) write(methodsCsv, row);
                    }
                }
                if (!callsBuf.isEmpty()) {
                    synchronized (callsLock) {
                        for (Object[] row : callsBuf) write(callsCsv, row);
                    }
                }

                parsedCounter.incrementAndGet();
            });

            log.info("Parsed {} files (total {}).", parsedCounter.get(), javaFiles.size());
        }

        log.info("CodeIndexService.indexProject finished -> {}", out);
    }

    public List<String> readStringArray(JsonNode p, String key) {
        if (p == null || !p.has(key) || !p.get(key).isArray()) return Collections.emptyList();
        List<String> list = new ArrayList<>();
        for (JsonNode n : p.get(key)) {
            if (n != null && n.isTextual()) list.add(n.asText());
        }
        return list;
    }

    private CombinedTypeSolver buildSolver(Path root, List<String> classpathEntries) {
        CombinedTypeSolver solver = new CombinedTypeSolver();
        solver.add(new ReflectionTypeSolver(false));
        solver.add(new JavaParserTypeSolver(root.toFile()));
        if (classpathEntries != null) {
            for (String cp : classpathEntries) {
                try {
                    Path p = Paths.get(cp);
                    if (Files.isRegularFile(p) && cp.toLowerCase(Locale.ROOT).endsWith(".jar")) {
                        solver.add(new JarTypeSolver(p));
                    } else if (Files.isDirectory(p)) {
                        solver.add(new JavaParserTypeSolver(p));
                    } else {
                        log.warn("Classpath entry ignored: {}", cp);
                    }
                } catch (Exception e) {
                    log.warn("Classpath entry can't be used by symbol solver: {}", cp, e);
                }
            }
        }
        return solver;
    }

    private List<Path> collectJavaFiles(Path root, PathFilter filter) throws IOException {
        List<Path> javaFiles = new ArrayList<>();
        try (Stream<Path> s = Files.walk(root)) {
            s.filter(p -> p.toString()
                    .endsWith(".java"))
                    .filter(p -> filter.accept(p))
                    .forEach(javaFiles::add);
        }
        return javaFiles;
    }

    private CSVPrinter csv(Path file, String... headers) throws IOException {
        Files.createDirectories(file.getParent());
        CSVFormat fmt = CSVFormat.DEFAULT.builder().setHeader(headers).build();
        return new CSVPrinter(new OutputStreamWriter(Files.newOutputStream(file), StandardCharsets.UTF_8), fmt);
    }

    private void write(CSVPrinter p, Object... vals) {
        try {
            p.printRecord(vals);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String kindOf(TypeDeclaration<?> td) {
        if (td.isClassOrInterfaceDeclaration()) {
            return td.asClassOrInterfaceDeclaration().isInterface() ? "interface" : "class";
        } else if (td.isEnumDeclaration()) {
            return "enum";
        } else if (td.isAnnotationDeclaration()) {
            return "annotation";
        }
        return td.getClass().getSimpleName();
    }

    private String extractExtends(TypeDeclaration<?> td) {
        if (td instanceof ClassOrInterfaceDeclaration) {
            ClassOrInterfaceDeclaration c = (ClassOrInterfaceDeclaration) td;
            return c.getExtendedTypes()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
        } else if (td instanceof EnumDeclaration) {
            return "";
        }
        return "";
    }

    private String extractImplements(TypeDeclaration<?> td) {
        if (td instanceof ClassOrInterfaceDeclaration) {
            ClassOrInterfaceDeclaration c = (ClassOrInterfaceDeclaration) td;
            NodeList<ClassOrInterfaceType> impls = c.getImplementedTypes();
            return impls.stream().map(Object::toString).collect(Collectors.joining(", "));
        }
        return "";
    }

    private String joinAnnotations(NodeList<AnnotationExpr> anns) {
        if (anns == null || anns.isEmpty()) return "";
        return anns.stream().map(Object::toString).collect(Collectors.joining(" | "));
    }

    private boolean hasDeprecated(NodeList<AnnotationExpr> anns) {
        if (anns == null) return false;
        for (AnnotationExpr a : anns) {
            String n = a.getNameAsString();
            if ("Deprecated".equals(n) || "java.lang.Deprecated".equals(n)) return true;
        }
        return false;
    }

    private String javadocSummary(BodyDeclaration<?> node) {
        return node.getComment()
                .filter(c -> c instanceof JavadocComment)
                .map(c -> (JavadocComment) c)
                .map(jc -> {
                    try {
                        Javadoc jd = jc.parse();
                        return jd.getDescription().toText().replaceAll("\\s+", " ").trim();
                    } catch (Exception e) {
                        return jc.getContent().replaceAll("\\s+", " ").trim();
                    }
                }).orElse("");
    }

    private String javadocSummary(TypeDeclaration<?> node) {
        return node.getComment()
                .filter(c -> c instanceof JavadocComment)
                .map(c -> (JavadocComment) c)
                .map(jc -> {
                    try {
                        Javadoc jd = jc.parse();
                        return jd.getDescription().toText().replaceAll("\\s+", " ").trim();
                    } catch (Exception e) {
                        return jc.getContent().replaceAll("\\s+", " ").trim();
                    }
                }).orElse("");
    }

    private int line(com.github.javaparser.ast.Node n) {
        return n.getRange().map(r -> r.begin.line).orElse(-1);
    }

    private String safeReadAllChars(Path f) {
        try {
            return new String(Files.readAllBytes(f), StandardCharsets.UTF_8);
        } catch (Exception ex) {
            // 回退：用平台默认编码（极少数情况下源文件不是 UTF-8）
            try {
                return new String(Files.readAllBytes(f));
            } catch (IOException e) {
                throw new RuntimeException("Cannot read file " + f, e);
            }
        }
    }
}