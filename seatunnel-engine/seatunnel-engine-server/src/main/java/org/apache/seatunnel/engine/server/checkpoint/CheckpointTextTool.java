/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import sun.misc.Unsafe;


/**
 * Utility CLI that converts SeaTunnel engine checkpoint snapshots between the native
 * Protostuff binary format and an editable JSON representation.
 *
 * <p>Usage:
 *
 * <pre>
 *   java org.apache.seatunnel.engine.server.checkpoint.CheckpointTextTool dump \
 *        /tmp/seatunnel/checkpoint_snapshot/<jobId>/<checkpointFile>.ser output.json
 *
 *   java org.apache.seatunnel.engine.server.checkpoint.CheckpointTextTool load \
 *        edited.json /tmp/seatunnel/checkpoint_snapshot/<jobId>/<checkpointFile>.ser
 * </pre>
 */
public final class CheckpointTextTool {

    private static final ProtoStuffSerializer SERIALIZER = new ProtoStuffSerializer();
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();
    private static final Unsafe UNSAFE = initUnsafe();
    private static final Path[] DEFAULT_LIB_DIRS = {
        Paths.get("seatunnel-dist", "target", "apache-seatunnel-2.3.13-SNAPSHOT", "lib"),
        Paths.get("seatunnel-dist", "target", "apache-seatunnel-2.3.13-SNAPSHOT", "connectors")
    };

    static {
        MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
        MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    }

    private CheckpointTextTool() {}

    public static void main(String[] args) throws Exception {
        args = new String[] {
            "--lib",
            "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/lib/",
            "--lib",
            "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/connectors/",
                "--lib",
                "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/starter/",
            "dump",
            "/opt/code/IDEAProjectOther/seatunnel/1760841955128-265-1-19.ser2",
            "/opt/code/IDEAProjectOther/seatunnel/12.json"
        };

//        args = new String[] {
//                "--lib",
//                "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/lib/",
//                "--lib",
//                "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/connectors/",
//                "--lib",
//                "/opt/code/IDEAProjectOther/seatunnel/seatunnel-dist/target/apache-seatunnel-2.3.13-SNAPSHOT/starter/",
//                "load",
//                "/opt/code/IDEAProjectOther/seatunnel/1.json",
//                "/opt/code/IDEAProjectOther/seatunnel/1760841955128-265-1-19.ser2"
//        };
        List<Path> extraLibs = new ArrayList<>();
        boolean disableAutoLib = false;

        int index = 0;
        while (index < args.length) {
            String arg = args[index];
            if ("--lib".equals(arg)) {
                index++;
                if (index >= args.length) {
                    printUsage();
                    System.exit(1);
                }
                extraLibs.add(Paths.get(args[index]));
                index++;
            } else if (arg.startsWith("--lib=")) {
                extraLibs.add(Paths.get(arg.substring("--lib=".length())));
                index++;
            } else if ("--no-auto-lib".equals(arg)) {
                disableAutoLib = true;
                index++;
            } else if (arg.startsWith("--")) {
                System.err.println("Unknown option: " + arg);
                printUsage();
                System.exit(1);
            } else {
                break;
            }
        }

        if (args.length - index != 3) {
            printUsage();
            System.exit(1);
        }

        String command = args[index];
        Path input = Paths.get(args[index + 1]);
        Path output = Paths.get(args[index + 2]);

        prepareClassLoader(extraLibs, !disableAutoLib);

        try {
            if ("dump".equalsIgnoreCase(command)) {
                dumpCheckpoint(input, output);
            } else if ("load".equalsIgnoreCase(command)) {
                loadCheckpoint(input, output);
            } else {
                printUsage();
                System.exit(1);
            }
        } catch (Exception e) {
            System.err.println("CheckpointTextTool failed: " + e.getMessage());
            throw e;
        }
    }

    private static void postProcessDecodedObject(StatePayloadText payload, Object original) {
        if (payload.object == null || original == null || !(payload.object instanceof Map<?, ?>)) {
            return;
        }
        processObjectFields(
                payload,
                castToStringObjectMap(payload.object),
                original,
                original.getClass(),
                new ArrayList<>());
    }

    private static void processObjectFields(
            StatePayloadText payload,
            Map<String, Object> structured,
            Object original,
            Class<?> currentClass,
            List<String> path) {
        if (currentClass == null || currentClass == Object.class) {
            return;
        }
        processObjectFields(payload, structured, original, currentClass.getSuperclass(), path);
        for (Field field : currentClass.getDeclaredFields()) {
            field.setAccessible(true);
            Object origValue;
            try {
                origValue = field.get(original);
            } catch (IllegalAccessException e) {
                continue;
            }
            Object structuredValue = structured.get(field.getName());
            if (origValue == null || structuredValue == null) {
                continue;
            }
            if (origValue instanceof Map && structuredValue instanceof Map) {
                Map<?, ?> origMap = (Map<?, ?>) origValue;
                if (isMapOfByteArray(origMap)) {
                    List<String> newPath = new ArrayList<>(path);
                    newPath.add(field.getName());
                    MapFieldInfo info = new MapFieldInfo(newPath, extractMapKeyClass(field));
                    Map<String, Object> decoded = new LinkedHashMap<>();
                    for (Map.Entry<?, ?> entry : origMap.entrySet()) {
                        String key = entry.getKey() == null ? null : entry.getKey().toString();
                        byte[] bytes = (byte[]) entry.getValue();
                        info.originalBytes.put(key, bytes);
                        decoded.put(key, decodeBytesToObject(bytes));
                    }
                    structured.put(field.getName(), decoded);
                    payload.mapFieldInfos.add(info);
                }
            } else if (structuredValue instanceof Map) {
                processObjectFields(
                        payload,
                        castToStringObjectMap(structuredValue),
                        origValue,
                        origValue.getClass(),
                        extendPath(path, field.getName()));
            }
        }
    }

    private static Object prepareObjectForSerialization(
            StatePayloadText payload, String className, Object object) throws Exception {
        if (object == null || payload.mapFieldInfos.isEmpty()) {
            return object;
        }
        if (object instanceof Map<?, ?>) {
            Map<String, Object> copy = deepCopyStringObjectMap(castToStringObjectMap(object));
            for (MapFieldInfo info : payload.mapFieldInfos) {
                if (info.path.isEmpty()) {
                    continue;
                }
                Map<String, Object> parent = navigateToParentMap(copy, info.path);
                if (parent == null) {
                    continue;
                }
                String fieldName = info.path.get(info.path.size() - 1);
                Object value = parent.get(fieldName);
                if (!(value instanceof Map<?, ?>)) {
                    continue;
                }
                Map<?, ?> valueMap = (Map<?, ?>) value;
                Map<Object, byte[]> encoded = new LinkedHashMap<>();
                for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
                    String key = entry.getKey() == null ? null : entry.getKey().toString();
                    Object entryValue = entry.getValue();
                    byte[] bytes;
                    if (entryValue == null) {
                        bytes = null;
                    } else if (entryValue instanceof byte[]) {
                        bytes = (byte[]) entryValue;
                    } else if (entryValue instanceof String) {
                        bytes = ((String) entryValue).getBytes(StandardCharsets.UTF_8);
                    } else {
                        bytes = MAPPER.writeValueAsBytes(entryValue);
                    }
                    Object keyObject = convertStringToKey(key, info.keyClass);
                    if (keyObject == null) {
                        keyObject = key;
                    }
                    encoded.put(keyObject, bytes);
                }
                parent.put(fieldName, encoded);
            }
            return copy;
        }
        return object;
    }

    private static void dumpCheckpoint(Path binaryFile, Path textFile) throws IOException {
        byte[] payload = Files.readAllBytes(binaryFile);
        PipelineState pipelineState = SERIALIZER.deserialize(payload, PipelineState.class);
        CompletedCheckpoint completedCheckpoint =
                SERIALIZER.deserialize(pipelineState.getStates(), CompletedCheckpoint.class);

        CheckpointDocument document = new CheckpointDocument();
        document.pipeline = PipelineInfo.from(pipelineState);
        document.checkpoint = CheckpointText.from(completedCheckpoint);

        Path parent = textFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        MAPPER.writeValue(textFile.toFile(), document);
    }

    private static void loadCheckpoint(Path textFile, Path binaryFile) throws IOException {
        CheckpointDocument document = MAPPER.readValue(textFile.toFile(), CheckpointDocument.class);
        CompletedCheckpoint checkpoint = document.checkpoint.toCheckpoint();
        byte[] checkpointBytes = SERIALIZER.serialize(checkpoint);

        PipelineState pipelineState =
                PipelineState.builder()
                        .jobId(document.pipeline.jobId)
                        .pipelineId(document.pipeline.pipelineId)
                        .checkpointId(document.pipeline.checkpointId)
                        .states(checkpointBytes)
                        .build();
        byte[] pipelineBytes = SERIALIZER.serialize(pipelineState);

        Path parent = binaryFile.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.write(binaryFile, pipelineBytes);
    }

    private static void printUsage() {
        System.err.println(
                "Usage: CheckpointTextTool [--lib <jar-or-dir>] [--no-auto-lib] dump <binary.ser> <output.json>");
        System.err.println(
                "   or: CheckpointTextTool [--lib <jar-or-dir>] [--no-auto-lib] load <input.json> <binary.ser>");
        System.err.println(
                "       --lib path       Add jar file or directory (all jars inside) to the temporary classpath");
        System.err.println(
                "       --no-auto-lib    Disable automatic library discovery via environment variables");
    }

    private static void prepareClassLoader(List<Path> cliLibs, boolean enableAutoLib)
            throws IOException {
        Set<Path> jarFiles = new HashSet<>();
        for (Path defaultDir : DEFAULT_LIB_DIRS) {
            collectJarFiles(defaultDir, jarFiles);
        }
        if (cliLibs != null) {
            for (Path path : cliLibs) {
                collectJarFiles(path, jarFiles);
            }
        }

        jarFiles.addAll(loadEnvConfiguredLibs());

        if (enableAutoLib) {
            jarFiles.addAll(discoverSeatunnelLibs());
        }

        if (jarFiles.isEmpty()) {
            return;
        }

        List<URL> urls = new ArrayList<>(jarFiles.size());
        for (Path jar : jarFiles) {
            urls.add(jar.toUri().toURL());
        }

        ClassLoader parent = Thread.currentThread().getContextClassLoader();
        URLClassLoader urlClassLoader = new URLClassLoader(urls.toArray(new URL[0]), parent);
        Thread.currentThread().setContextClassLoader(urlClassLoader);
    }

    private static Set<Path> loadEnvConfiguredLibs() throws IOException {
        Set<Path> result = new HashSet<>();
        String envList = System.getenv("SEATUNNEL_CHECKPOINT_TOOL_LIBS");
        if (envList != null) {
            for (String segment : splitPathList(envList)) {
                collectJarFiles(Paths.get(segment), result);
            }
        }

        String sysProperty = System.getProperty("seatunnel.checkpoint.tool.libs");
        if (sysProperty != null) {
            for (String segment : splitPathList(sysProperty)) {
                collectJarFiles(Paths.get(segment), result);
            }
        }
        return result;
    }

    private static Set<Path> discoverSeatunnelLibs() throws IOException {
        Set<Path> result = new HashSet<>();
        String home = System.getenv("SEATUNNEL_HOME");
        if (home != null) {
            Path homePath = Paths.get(home);
            collectJarFiles(homePath.resolve("lib"), result);
            Path connectors = homePath.resolve("connectors");
            if (Files.isDirectory(connectors)) {
                try (DirectoryStream<Path> dirs = Files.newDirectoryStream(connectors)) {
                    for (Path dir : dirs) {
                        collectJarFiles(dir, result);
                    }
                }
            }
        }

        Path distLib = Paths.get("seatunnel-dist", "target");
        if (Files.exists(distLib)) {
            try (DirectoryStream<Path> sub = Files.newDirectoryStream(distLib)) {
                for (Path dir : sub) {
                    if (Files.isDirectory(dir) && dir.getFileName().toString().startsWith("seatunnel")) {
                        collectJarFiles(dir.resolve("lib"), result);
                        collectJarFiles(dir.resolve("connectors"), result);
                    }
                }
            }
        }
        return result;
    }

    private static void collectJarFiles(Path path, Set<Path> collector) throws IOException {
        if (path == null || !Files.exists(path)) {
            return;
        }
        if (Files.isRegularFile(path) && path.toString().endsWith(".jar")) {
            collector.add(path.toRealPath());
            return;
        }
        if (Files.isDirectory(path)) {
            try (Stream<Path> stream = Files.walk(path)) {
                stream.filter(p -> Files.isRegularFile(p) && p.toString().endsWith(".jar"))
                        .forEach(
                                p -> {
                                    try {
                                        collector.add(p.toRealPath());
                                    } catch (IOException e) {
                                        // ignore broken entries
                                    }
                                });
            }
        }
    }

    private static List<String> splitPathList(String value) {
        if (value == null || value.isEmpty()) {
            return new ArrayList<>();
        }
        String[] segments = value.split("[:;]");
        List<String> result = new ArrayList<>(segments.length);
        for (String segment : segments) {
            if (!segment.isEmpty()) {
                result.add(segment);
            }
        }
        return result;
    }

    private static class CheckpointDocument {
        private PipelineInfo pipeline;
        private CheckpointText checkpoint;
    }

    private static class PipelineInfo {
        private String jobId;
        private int pipelineId;
        private long checkpointId;

        private static PipelineInfo from(PipelineState pipelineState) {
            PipelineInfo info = new PipelineInfo();
            info.jobId = pipelineState.getJobId();
            info.pipelineId = pipelineState.getPipelineId();
            info.checkpointId = pipelineState.getCheckpointId();
            return info;
        }
    }

    @NoArgsConstructor
    private static class CheckpointText {
        private long jobId;
        private int pipelineId;
        private long checkpointId;
        private long triggerTimestamp;
        private String checkpointType;
        private long completedTimestamp;
        private boolean restored;
        private List<TaskStateText> taskStates = new ArrayList<>();
        private List<TaskStatisticsText> taskStatistics = new ArrayList<>();

        private static CheckpointText from(CompletedCheckpoint checkpoint) {
            CheckpointText text = new CheckpointText();
            text.jobId = checkpoint.getJobId();
            text.pipelineId = checkpoint.getPipelineId();
            text.checkpointId = checkpoint.getCheckpointId();
            text.triggerTimestamp = checkpoint.getCheckpointTimestamp();
            text.checkpointType = checkpoint.getCheckpointType().name();
            text.completedTimestamp = checkpoint.getCompletedTimestamp();
            text.restored = checkpoint.isRestored();

            checkpoint.getTaskStates()
                    .forEach(
                            (key, state) ->
                                    text.taskStates.add(TaskStateText.from(key, state)));
            checkpoint.getTaskStatistics()
                    .forEach(
                            (vertexId, statistics) ->
                                    text.taskStatistics.add(
                                            TaskStatisticsText.from(vertexId, statistics)));

            text.taskStates.sort(Comparator.comparing(ts -> ts.stateKey));
            text.taskStatistics.sort(Comparator.comparingLong(ts -> ts.jobVertexId));
            return text;
        }

        private CompletedCheckpoint toCheckpoint() {
            Map<ActionStateKey, ActionState> states = new LinkedHashMap<>();
            for (TaskStateText taskStateText : safeList(taskStates)) {
                ActionStateKey key = new ActionStateKey(taskStateText.stateKey);
                ActionState actionState = new ActionState(key, taskStateText.parallelism);
                if (taskStateText.coordinatorState != null) {
                    actionState.reportState(-1, taskStateText.coordinatorState.toActionSubtaskState());
                }
                for (SubtaskStateText sub : safeList(taskStateText.subtaskStates)) {
                    actionState.reportState(sub.index, sub.toActionSubtaskState());
                }
                states.put(key, actionState);
            }

            Map<Long, TaskStatistics> statisticsMap = new LinkedHashMap<>();
            for (TaskStatisticsText statisticsText : safeList(taskStatistics)) {
                int parallelism = statisticsText.parallelism;
                if (parallelism <= 0) {
                    int completedSize = safeList(statisticsText.subtaskCompleted).size();
                    int maxIndex =
                            safeList(statisticsText.subtaskStats).stream()
                                    .mapToInt(stat -> stat.subtaskIndex + 1)
                                    .max()
                                    .orElse(0);
                    parallelism = Math.max(completedSize, maxIndex);
                    if (parallelism <= 0) {
                        parallelism = 1;
                    }
                }
                TaskStatistics statistics =
                        new TaskStatistics(statisticsText.jobVertexId, parallelism);
                for (SubtaskStatisticsText subtaskStatisticsText :
                        safeList(statisticsText.subtaskStats)) {
                    statistics.reportSubtaskStatistics(subtaskStatisticsText.toSubtaskStatistics());
                }
                List<Boolean> completed = safeList(statisticsText.subtaskCompleted);
                for (int i = 0; i < completed.size(); i++) {
                    if (Boolean.TRUE.equals(completed.get(i))) {
                        statistics.completed(i);
                    }
                }
                statisticsMap.put(statisticsText.jobVertexId, statistics);
            }

            CheckpointType type =
                    checkpointType == null
                            ? CheckpointType.CHECKPOINT_TYPE
                            : CheckpointType.valueOf(checkpointType);
            return new CompletedCheckpoint(
                    jobId,
                    pipelineId,
                    checkpointId,
                    triggerTimestamp,
                    type,
                    completedTimestamp,
                    states,
                    statisticsMap);
        }
    }

    @NoArgsConstructor
    private static class TaskStateText {
        private String stateKey;
        private int parallelism;
        private SubtaskStateText coordinatorState;
        private List<SubtaskStateText> subtaskStates = new ArrayList<>();

        private static TaskStateText from(ActionStateKey key, ActionState actionState) {
            TaskStateText text = new TaskStateText();
            text.stateKey = key.getName();
            text.parallelism = actionState.getParallelism();

            if (actionState.getCoordinatorState() != null) {
                text.coordinatorState = SubtaskStateText.from(actionState.getCoordinatorState());
            }

            if (actionState.getSubtaskStates() != null) {
                actionState.getSubtaskStates().stream()
                        .filter(Objects::nonNull)
                        .map(SubtaskStateText::from)
                        .sorted(Comparator.comparingInt(sub -> sub.index))
                        .forEach(text.subtaskStates::add);
            }
            return text;
        }
    }

    @NoArgsConstructor
    private static class SubtaskStateText {
        private String stateKey;
        private int index;
        private List<StatePayloadText> states;

        private static SubtaskStateText from(ActionSubtaskState subtaskState) {
            SubtaskStateText text = new SubtaskStateText();
            text.stateKey =
                    subtaskState.getStateKey() == null
                            ? null
                            : subtaskState.getStateKey().getName();
            text.index = subtaskState.getIndex();
            text.states = encodeStateList(subtaskState.getState());
            return text;
        }

        private ActionSubtaskState toActionSubtaskState() {
            ActionStateKey key = stateKey == null ? null : new ActionStateKey(stateKey);
            return new ActionSubtaskState(key, index, decodeStateList(states));
        }
    }

    @NoArgsConstructor
    private static class TaskStatisticsText {
        private long jobVertexId;
        private int parallelism;
        private List<SubtaskStatisticsText> subtaskStats = new ArrayList<>();
        private List<Boolean> subtaskCompleted = new ArrayList<>();

        private static TaskStatisticsText from(Long jobVertexId, TaskStatistics statistics) {
            TaskStatisticsText text = new TaskStatisticsText();
            text.jobVertexId = jobVertexId;

            List<SubtaskStatistics> sourceStats = statistics.getSubtaskStats();
            text.parallelism = sourceStats.size();
            sourceStats.stream()
                    .filter(Objects::nonNull)
                    .map(SubtaskStatisticsText::from)
                    .sorted(Comparator.comparingInt(stat -> stat.subtaskIndex))
                    .forEach(text.subtaskStats::add);

            boolean[] completedFlags = extractCompletedFlags(statistics);
            for (boolean flag : completedFlags) {
                text.subtaskCompleted.add(flag);
            }
            return text;
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class SubtaskStatisticsText {
        private int subtaskIndex;
        private long ackTimestamp;
        private long stateSize;
        private String status;

        private static SubtaskStatisticsText from(SubtaskStatistics statistics) {
            return new SubtaskStatisticsText(
                    statistics.getSubtaskIndex(),
                    statistics.getAckTimestamp(),
                    statistics.getStateSize(),
                    statistics.getSubtaskStatus().name());
        }

        private SubtaskStatistics toSubtaskStatistics() {
            return new SubtaskStatistics(
                    subtaskIndex, ackTimestamp, stateSize, SubtaskStatus.valueOf(status));
        }
    }

    private static <T> List<T> safeList(List<T> list) {
        return list == null ? new ArrayList<>() : list;
    }

    private static List<StatePayloadText> encodeStateList(List<byte[]> states) {
        if (states == null) {
            return null;
        }
        List<StatePayloadText> results = new ArrayList<>(states.size());
        for (byte[] bytes : states) {
            if (bytes == null) {
                results.add(null);
            } else {
                results.add(StatePayloadText.from(bytes));
            }
        }
        return results;
    }

    private static List<byte[]> decodeStateList(List<StatePayloadText> states) {
        if (states == null) {
            return new ArrayList<>();
        }
        List<byte[]> result = new ArrayList<>(states.size());
        for (StatePayloadText state : states) {
            result.add(state == null ? null : state.toBytes());
        }
        return result;
    }

    private static boolean[] extractCompletedFlags(TaskStatistics statistics) {
        try {
            Field field = TaskStatistics.class.getDeclaredField("subtaskCompleted");
            field.setAccessible(true);
            return (boolean[]) field.get(statistics);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to access TaskStatistics.subtaskCompleted", e);
        }
    }

    @NoArgsConstructor
    @com.fasterxml.jackson.annotation.JsonInclude(
            com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    private static class StatePayloadText {
        private String className;
        private Object object;
        private String base64;
        private String error;
        private transient byte[] originalBytes;
        @JsonIgnore private final List<MapFieldInfo> mapFieldInfos = new ArrayList<>();
        @JsonIgnore private SpecialCollectionType collectionType;
        @JsonIgnore private boolean nonEditable;

        private static StatePayloadText from(byte[] bytes) {
            StatePayloadText payload = new StatePayloadText();
            payload.base64 = BASE64_ENCODER.encodeToString(bytes);
            payload.originalBytes = bytes;
            JavaObjectResult result = deserializeJavaObject(bytes);
            if (result.className != null) {
                payload.className = result.className;
            }
            if (result.error == null) {
                if (result.value != null) {
                    try {
                        payload.object = convertObjectToTree(result.value);
                        if (payload.className != null) {
                            if (payload.className.equals("java.util.Collections$EmptyList")) {
                                payload.collectionType = SpecialCollectionType.EMPTY_LIST;
                                payload.object =
                                        payload.object instanceof List
                                                ? new ArrayList<>((List<?>) payload.object)
                                                : new ArrayList<>();
                                payload.base64 = null;
                            } else if (payload.className.equals("java.util.Collections$EmptySet")) {
                                payload.collectionType = SpecialCollectionType.EMPTY_SET;
                                payload.object =
                                        payload.object instanceof Collection
                                                ? new ArrayList<>((Collection<?>) payload.object)
                                                : new ArrayList<>();
                                payload.base64 = null;
                            } else if (payload.className.equals("java.util.Collections$EmptyMap")) {
                                payload.collectionType = SpecialCollectionType.EMPTY_MAP;
                                payload.object =
                                        payload.object instanceof Map
                                                ? new LinkedHashMap<>((Map<?, ?>) payload.object)
                                                : new LinkedHashMap<>();
                                payload.base64 = null;
                            }
                        }
                        postProcessDecodedObject(payload, result.value);
                        boolean editable =
                                payload.object != null
                                        && isEditableState(result.value, payload.className);
                        if (editable && !isJdkLockClass(payload.className)) {
                            payload.base64 = null;
                        } else {
                            payload.nonEditable = true;
                            if (payload.base64 == null) {
                                payload.base64 = BASE64_ENCODER.encodeToString(bytes);
                            }
                            if (payload.error == null) {
                                payload.error =
                                        "Binary state preserved; class "
                                                + payload.className
                                                + " lacks editable constructor";
                            }
                        }
                    } catch (IllegalArgumentException e) {
                        handleConversionException(payload, e);
                    }
                }
            } else {
                handleDeserializationException(payload, result.error);
            }
            return payload;
        }

        private byte[] toBytes() {
            if (originalBytes == null && base64 != null) {
                originalBytes = BASE64_DECODER.decode(base64);
            }
            if (!nonEditable && !isEditableState(this.object, this.className)) {
                nonEditable = true;
            }
            if (nonEditable) {
                if (base64 != null) {
                    return BASE64_DECODER.decode(base64);
                }
                return originalBytes;
            }
            if (object != null && className != null) {
                if (collectionType != null) {
                    Object preparedCollection =
                            prepareSpecialCollectionForSerialization(collectionType, object);
                    if (preparedCollection != null) {
                        try {
                            return serializeJavaObject(preparedCollection);
                        } catch (Exception e) {
                            if (originalBytes != null) {
                                return originalBytes;
                            }
                            throw new IllegalArgumentException(
                                    "Failed to serialize special collection for class "
                                            + className,
                                    e);
                        }
                    }
                }
                try {
                    Object prepared = prepareObjectForSerialization(this, className, object);
                    try {
                        ClassLoader loader = Thread.currentThread().getContextClassLoader();
                        Class<?> clazz = Class.forName(className, false, loader);
                        Object value = convertValueWithConstructors(prepared, clazz);
                        return serializeJavaObject(value);
                    } catch (ClassNotFoundException e) {
                        if (originalBytes != null) {
                            return originalBytes;
                        }
                        if (base64 != null) {
                            return BASE64_DECODER.decode(base64);
                        }
                        throw e;
                    } catch (FallbackOriginalException fallback) {
                        if (originalBytes != null) {
                            if (base64 == null) {
                                base64 = BASE64_ENCODER.encodeToString(originalBytes);
                            }
                            this.nonEditable = true;
                            this.error =
                                    "Binary state preserved for class "
                                            + className
                                            + ": "
                                            + fallback.getCause().getMessage();
                            return originalBytes;
                        }
                        throw new IllegalArgumentException(
                                "Failed to serialize state object for class " + className,
                                fallback);
                    }
                } catch (Exception e) {
                    if (originalBytes != null) {
                        return originalBytes;
                    }
                    throw new IllegalArgumentException(
                            "Failed to serialize state object for class " + className, e);
                }
            }
            if (base64 != null) {
                return BASE64_DECODER.decode(base64);
            }
            return originalBytes;
        }
    }

    private static Object convertValueWithConstructors(Object value, Class<?> targetClass)
            throws FallbackOriginalException {
        if (value == null) {
            return null;
        }
        try {
            return MAPPER.convertValue(value, targetClass);
        } catch (IllegalArgumentException e) {
            throw new FallbackOriginalException(e);
        }
    }

    private static void handleDeserializationException(StatePayloadText payload, Exception error) {
        if (error instanceof ClassNotFoundException) {
            if (handleSpecialCollectionFallback(payload)) {
                return;
            }
            payload.error =
                    "Missing class: "
                            + (payload.className != null
                                    ? payload.className
                                    : error.getMessage());
            return;
        }
        if (isJdkLockClass(payload.className)) {
            payload.object = createJdkLockPlaceholder();
            return;
        }
        payload.error = error.getClass().getSimpleName() + ": " + error.getMessage();
    }

    private static void handleConversionException(StatePayloadText payload, Exception error) {
        if (isJdkLockClass(payload.className)) {
            payload.object = createJdkLockPlaceholder();
            return;
        }
        Throwable cause = error.getCause();
        payload.error =
                (cause != null ? cause.getClass().getSimpleName() + ": " + cause.getMessage() :
                        error.getClass().getSimpleName() + ": " + error.getMessage());
    }

    private static boolean isJdkLockClass(String className) {
        return className != null
                && className.startsWith("java.util.concurrent.locks.");
    }

    private static boolean isEditableState(Object value, String className) {
        if (value == null || className == null) {
            return false;
        }
        try {
            Class<?> clazz = value.getClass();
            if (clazz.isPrimitive()
                    || clazz.getName().startsWith("java.")
                    || Collection.class.isAssignableFrom(clazz)
                    || Map.class.isAssignableFrom(clazz)) {
                return true;
            }
            for (Constructor<?> ctor : clazz.getDeclaredConstructors()) {
                if (ctor.getParameterCount() == 0) {
                    return true;
                }
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    private static Map<String, Object> createJdkLockPlaceholder() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("note", "JDK lock state preserved; editing is not supported");
        return info;
    }

    private static boolean handleSpecialCollectionFallback(StatePayloadText payload) {
        if (payload.className == null) {
            return false;
        }
        switch (payload.className) {
            case "java.util.Collections$EmptyList":
                payload.collectionType = SpecialCollectionType.EMPTY_LIST;
                payload.object = new ArrayList<>();
                payload.base64 = null;
                payload.error = null;
                return true;
            case "java.util.Collections$EmptySet":
                payload.collectionType = SpecialCollectionType.EMPTY_SET;
                payload.object = new ArrayList<>();
                payload.base64 = null;
                payload.error = null;
                return true;
            case "java.util.Collections$EmptyMap":
                payload.collectionType = SpecialCollectionType.EMPTY_MAP;
                payload.object = new LinkedHashMap<>();
                payload.base64 = null;
                payload.error = null;
                return true;
            default:
                return false;
        }
    }

    private static Object prepareSpecialCollectionForSerialization(
            SpecialCollectionType type, Object structuredValue) {
        switch (type) {
            case EMPTY_LIST:
                {
                    List<?> list = structuredValue instanceof List ? (List<?>) structuredValue : null;
                    if (list == null || list.isEmpty()) {
                        return java.util.Collections.emptyList();
                    }
                    return new ArrayList<>(list);
                }
            case EMPTY_SET:
                {
                    List<?> list = structuredValue instanceof List ? (List<?>) structuredValue : null;
                    if (list == null || list.isEmpty()) {
                        return java.util.Collections.emptySet();
                    }
                    return new java.util.HashSet<>(list);
                }
            case EMPTY_MAP:
                {
                    Map<?, ?> map =
                            structuredValue instanceof Map ? (Map<?, ?>) structuredValue : null;
                    if (map == null || map.isEmpty()) {
                        return java.util.Collections.emptyMap();
                    }
                    return new LinkedHashMap<>(map);
                }
            default:
                return null;
        }
    }

    private static JavaObjectResult deserializeJavaObject(byte[] bytes) {
        ContextObjectInputStream inputStream = null;
        try {
            inputStream = new ContextObjectInputStream(new ByteArrayInputStream(bytes));
            Object value = inputStream.readObject();
            String className =
                    value != null ? value.getClass().getName() : inputStream.getLastDescriptorName();
            return new JavaObjectResult(value, className, null);
        } catch (Exception e) {
            String className = inputStream != null ? inputStream.getLastDescriptorName() : null;
            return new JavaObjectResult(null, className, e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    private static byte[] serializeJavaObject(Object value) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            objectOutputStream.writeObject(value);
        }
        return outputStream.toByteArray();
    }

    private static Object convertObjectToTree(Object value) {
        return MAPPER.convertValue(value, Object.class);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castToStringObjectMap(Object value) {
        return (Map<String, Object>) value;
    }

    private static List<String> extendPath(List<String> base, String fieldName) {
        List<String> newPath = new ArrayList<>(base.size() + 1);
        newPath.addAll(base);
        newPath.add(fieldName);
        return newPath;
    }

    private static boolean isMapOfByteArray(Map<?, ?> map) {
        if (map == null) {
            return false;
        }
        for (Object value : map.values()) {
            if (value != null && !(value instanceof byte[])) {
                return false;
            }
        }
        return true;
    }

    private static Object decodeBytesToObject(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return MAPPER.readValue(bytes, Object.class);
        } catch (Exception e) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> deepCopyStringObjectMap(Map<String, Object> map)
            throws IOException {
        return MAPPER.readValue(MAPPER.writeValueAsBytes(map), Map.class);
    }

    private static Class<?> extractMapKeyClass(Field field) {
        Type type = field.getGenericType();
        if (type instanceof ParameterizedType) {
            Type keyType = ((ParameterizedType) type).getActualTypeArguments()[0];
            if (keyType instanceof Class<?>) {
                return (Class<?>) keyType;
            }
        }
        return String.class;
    }

    private static Map<String, Object> navigateToParentMap(
            Map<String, Object> root, List<String> path) {
        Map<String, Object> current = root;
        for (int i = 0; i < path.size() - 1; i++) {
            Object next = current.get(path.get(i));
            if (!(next instanceof Map)) {
                return null;
            }
            current = castToStringObjectMap(next);
        }
        return current;
    }

    private static Object convertStringToKey(String key, Class<?> keyClass) {
        if (keyClass == null || keyClass == Object.class || keyClass == String.class) {
            return key;
        }
        if (key == null) {
            return null;
        }
        try {
            Method parse = keyClass.getMethod("parse", String.class);
            return parse.invoke(null, key);
        } catch (Exception ignored) {
        }
        try {
            Method valueOf = keyClass.getMethod("valueOf", String.class);
            return valueOf.invoke(null, key);
        } catch (Exception ignored) {
        }
        try {
            Method of = keyClass.getMethod("of", String.class);
            return of.invoke(null, key);
        } catch (Exception ignored) {
        }
        try {
            Method fromString = keyClass.getMethod("fromString", String.class);
            return fromString.invoke(null, key);
        } catch (Exception ignored) {
        }
        try {
            Constructor<?> constructor = keyClass.getConstructor(String.class);
            return constructor.newInstance(key);
        } catch (Exception ignored) {
        }
        if (Enum.class.isAssignableFrom(keyClass)) {
            @SuppressWarnings("unchecked")
            Class<? extends Enum> enumClass = (Class<? extends Enum>) keyClass;
            return Enum.valueOf(enumClass, key);
        }
        return key;
    }

    private static Object instantiateByFields(Map<String, Object> map, Class<?> targetClass)
            throws FallbackOriginalException {
        if (UNSAFE == null
                || java.lang.reflect.Modifier.isAbstract(targetClass.getModifiers())) {
            throw new FallbackOriginalException(
                    new InstantiationException(
                            "Cannot instantiate " + targetClass.getName()));
        }
        Object instance;
        try {
            instance = UNSAFE.allocateInstance(targetClass);
        } catch (InstantiationException ex) {
            throw new FallbackOriginalException(ex);
        }
        for (Field field : collectAllFields(targetClass)) {
            field.setAccessible(true);
            if (!map.containsKey(field.getName())) {
                continue;
            }
            Object rawValue = map.get(field.getName());
            try {
                Object converted = convertStructuredValue(rawValue, field.getGenericType());
                field.set(instance, converted);
            } catch (FallbackOriginalException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new FallbackOriginalException(ex);
            }
        }
        return instance;
    }

    private static List<Field> collectAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = clazz;
        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    fields.add(field);
                }
            }
            current = current.getSuperclass();
        }
        return fields;
    }

    private static Object convertStructuredValue(Object value, Type targetType)
            throws FallbackOriginalException {
        if (value == null) {
            return null;
        }
        try {
            if (targetType instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) targetType;
                Type raw = pt.getRawType();
                if (raw instanceof Class && Collection.class.isAssignableFrom((Class<?>) raw)) {
                    return convertCollection(value, (Class<?>) raw, pt.getActualTypeArguments()[0]);
                }
                if (raw instanceof Class && Map.class.isAssignableFrom((Class<?>) raw)) {
                    return convertMap(
                            value,
                            (Class<?>) raw,
                            pt.getActualTypeArguments()[0],
                            pt.getActualTypeArguments()[1]);
                }
            }
            if (targetType instanceof Class<?>) {
                return convertToClass(value, (Class<?>) targetType);
            }
            return MAPPER.convertValue(value, new com.fasterxml.jackson.core.type.TypeReference<Object>() {});
        } catch (FallbackOriginalException e) {
            throw e;
        } catch (Exception e) {
            throw new FallbackOriginalException(e);
        }
    }

    private static Object convertToClass(Object value, Class<?> targetClass)
            throws FallbackOriginalException {
        if (value == null) {
            return null;
        }
        if (targetClass.isAssignableFrom(value.getClass())) {
            return value;
        }
        if (targetClass == String.class
                || targetClass.isPrimitive()
                || Number.class.isAssignableFrom(targetClass)
                || Boolean.class.isAssignableFrom(targetClass)
                || Character.class.isAssignableFrom(targetClass)) {
            return MAPPER.convertValue(value, targetClass);
        }
        if (targetClass.isEnum()) {
            return Enum.valueOf((Class) targetClass, value.toString());
        }
        if (Collection.class.isAssignableFrom(targetClass)) {
            return convertCollection(value, targetClass, Object.class);
        }
        if (Map.class.isAssignableFrom(targetClass)) {
            return convertMap(value, targetClass, Object.class, Object.class);
        }
        if (value instanceof Map && UNSAFE != null) {
            return instantiateByFields(castToStringObjectMap(value), targetClass);
        }
        try {
            return MAPPER.convertValue(value, targetClass);
        } catch (IllegalArgumentException e) {
            throw new FallbackOriginalException(e);
        }
    }

    private static Object convertCollection(Object value, Class<?> rawClass, Type elementType)
            throws FallbackOriginalException {
        if (!(value instanceof Collection)) {
            throw new FallbackOriginalException(
                    new IllegalArgumentException("Expected collection for " + rawClass.getName()));
        }
        Collection<?> source = (Collection<?>) value;
        Collection<Object> target;
        if (rawClass.isInterface() || java.lang.reflect.Modifier.isAbstract(rawClass.getModifiers())) {
            target = new ArrayList<>(source.size());
        } else {
            try {
                target = (Collection<Object>) rawClass.newInstance();
            } catch (Exception ex) {
                target = new ArrayList<>(source.size());
            }
        }
        for (Object item : source) {
            target.add(convertStructuredValue(item, elementType));
        }
        return target;
    }

    private static Object convertMap(Object value, Class<?> rawClass, Type keyType, Type valueType)
            throws FallbackOriginalException {
        if (!(value instanceof Map)) {
            throw new FallbackOriginalException(
                    new IllegalArgumentException("Expected map for " + rawClass.getName()));
        }
        Map<?, ?> source = (Map<?, ?>) value;
        Map<Object, Object> target;
        if (rawClass.isInterface() || java.lang.reflect.Modifier.isAbstract(rawClass.getModifiers())) {
            target = new LinkedHashMap<>();
        } else {
            try {
                target = (Map<Object, Object>) rawClass.newInstance();
            } catch (Exception ex) {
                target = new LinkedHashMap<>();
            }
        }
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            Object key = convertStructuredValue(entry.getKey(), keyType);
            Object val = convertStructuredValue(entry.getValue(), valueType);
            target.put(key, val);
        }
        return target;
    }

    private static Unsafe initUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Throwable t) {
            return null;
        }
    }

    private static class FallbackOriginalException extends Exception {
        FallbackOriginalException(Throwable cause) {
            super(cause);
        }
    }

    private static class MapFieldInfo {
        private final List<String> path;
        private final Class<?> keyClass;
        private final Map<String, byte[]> originalBytes = new LinkedHashMap<>();

        MapFieldInfo(List<String> path, Class<?> keyClass) {
            this.path = path;
            this.keyClass = keyClass == null ? String.class : keyClass;
        }
    }

    private enum SpecialCollectionType {
        EMPTY_LIST,
        EMPTY_SET,
        EMPTY_MAP
    }

    private static class ContextObjectInputStream extends ObjectInputStream {
        private String lastDescriptorName;

        ContextObjectInputStream(ByteArrayInputStream in) throws IOException {
            super(in);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc)
                throws IOException, ClassNotFoundException {
            lastDescriptorName = desc.getName();
            try {
                return Class.forName(
                        desc.getName(), false, Thread.currentThread().getContextClassLoader());
            } catch (ClassNotFoundException e) {
                return super.resolveClass(desc);
            }
        }

        String getLastDescriptorName() {
            return lastDescriptorName;
        }
    }

    private static class JavaObjectResult {
        private final Object value;
        private final String className;
        private final Exception error;

        private JavaObjectResult(Object value, String className, Exception error) {
            this.value = value;
            this.className = className;
            this.error = error;
        }
    }
}
