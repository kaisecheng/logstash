package org.logstash.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches files for changes using the OS-level NIO {@link WatchService} and dispatches
 * {@link FileChangeCallback} notifications to registered listeners.
 *
 * <p>WatchService monitors <em>directories</em>, not individual files. This class watches
 * the parent directory of each registered file and matches events against the registered
 * file set. Both {@code ENTRY_CREATE} and {@code ENTRY_MODIFY} events are watched.
 *
 * <p>The background watcher thread is lazily started on the first {@link #register} call.
 * If no files are ever registered the thread is never created.
 */
public final class FileWatchService implements Closeable {

    private static final Logger logger = LogManager.getLogger(FileWatchService.class);

    // Sentinel kind fired to all callbacks when their parent directory's WatchKey becomes invalid.
    // Callers that need to react to watch loss (e.g. re-register or alert) should check for this kind.
    public static final WatchEvent.Kind<Path> WATCH_LOST = new WatchEvent.Kind<Path>() {
        @Override public String name() { return "WATCH_LOST"; }
        @Override public Class<Path> type() { return Path.class; }
    };

    // Callback invoked on the watcher thread when a watched file changes
    @FunctionalInterface
    public interface FileChangeCallback {
        void onChange(FileChangeEvent event);
    }

    // Carries the absolute path and event kind, ENTRY_CREATE and ENTRY_MODIFY, for a file change notification
    public static final class FileChangeEvent {
        private final Path path;
        private final WatchEvent.Kind<?> kind;

        FileChangeEvent(final Path path, final WatchEvent.Kind<?> kind) {
            this.path = path;
            this.kind = kind;
        }

        public Path path() { return path; }
        public WatchEvent.Kind<?> kind() { return kind; }
    }

    private static final class WatchedDir {
        final WatchKey key;
        final CopyOnWriteArrayList<Path> files = new CopyOnWriteArrayList<>();

        WatchedDir(final WatchKey key) {
            this.key = key;
        }
    }

    private final WatchService watchService;
    // absolute directory -> WatchedDir(WatchKey, registered file paths)
    private final ConcurrentHashMap<Path, WatchedDir> watchedDirs = new ConcurrentHashMap<>();
    // absolute file path -> callbacks
    private final ConcurrentHashMap<Path, CopyOnWriteArrayList<FileChangeCallback>> filepathCallbacks = new ConcurrentHashMap<>();
    private volatile Thread watcherThread;
    // guards a single start of watcherThread
    private final AtomicBoolean started = new AtomicBoolean(false);

    private FileWatchService(final WatchService watchService) {
        this.watchService = watchService;
    }

    public static FileWatchService create() throws IOException {
        return new FileWatchService(FileSystems.getDefault().newWatchService());
    }

    /**
     * Registers {@code callback} to be invoked whenever {@code filePath} changes.
     * Multiple callbacks may be registered for the same path. If the parent directory
     * is not yet watched, a new {@link WatchKey} is created for it. The watcher thread
     * is started on the first call.
     */
    public synchronized void register(final Path filePath, final FileChangeCallback callback) throws IOException {
        final Path fileAbsPath = filePath.toAbsolutePath();
        CopyOnWriteArrayList<FileChangeCallback> callbacks = filepathCallbacks.computeIfAbsent(fileAbsPath, k -> new CopyOnWriteArrayList<>());
        callbacks.add(callback);

        final Path dir = fileAbsPath.getParent();
        WatchedDir watchedDir = watchedDirs.get(dir);
        if (watchedDir == null) {
            final WatchKey key = dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            watchedDir = new WatchedDir(key);
            watchedDirs.put(dir, watchedDir);
            logger.debug("Watching directory {}", dir);
        }
        if (!watchedDir.files.contains(fileAbsPath)) {
            watchedDir.files.add(fileAbsPath);
            logger.debug("Watching file {}", fileAbsPath);
        }
        if (started.compareAndSet(false, true)) {
            watcherThread = new Thread(this::watcherLoop, "core-file-watch-service");
            watcherThread.setDaemon(true);
            watcherThread.start();
            logger.info("Watcher thread started");
        }
    }

    /**
     * Removes {@code callback} for {@code filePath}. When the last callback for a file
     * is removed its parent directory's {@link WatchKey} is cancelled if no other files
     * in that directory remain watched.
     */
    public synchronized void deregister(final Path filePath, final FileChangeCallback callback) {
        final Path fileAbsPath = filePath.toAbsolutePath();
        final CopyOnWriteArrayList<FileChangeCallback> callbacks = filepathCallbacks.get(fileAbsPath);
        if (callbacks == null) return;

        callbacks.remove(callback);
        if (callbacks.isEmpty()) {
            filepathCallbacks.remove(fileAbsPath);
            final Path dir = fileAbsPath.getParent();
            final WatchedDir watchedDir = watchedDirs.get(dir);
            if (watchedDir != null) {
                watchedDir.files.remove(fileAbsPath);
                if (watchedDir.files.isEmpty()) {
                    watchedDirs.remove(dir);
                    watchedDir.key.cancel();
                    logger.debug("Stopped watching directory {}", dir);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        watchService.close();
        if (watcherThread != null) {
            try {
                watcherThread.join(5_000L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void watcherLoop() {
        while (true) {
            final WatchKey key;
            try {
                key = watchService.take();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (final ClosedWatchServiceException e) {
                return;
            }

            final Path dir = (Path) key.watchable();
            for (final WatchEvent<?> event : key.pollEvents()) {
                if (event.kind() == StandardWatchEventKinds.OVERFLOW) continue;
                final Path absPath = dir.resolve((Path) event.context()).toAbsolutePath();
                fireCallbacks(absPath, event.kind());
            }
            if (!key.reset()) {
                logger.warn("Watched directory {} is no longer accessible; file change detection lost for files in that directory.", dir);
                final WatchedDir watchedDir = watchedDirs.remove(dir);
                if (watchedDir != null) {
                    for (final Path f : watchedDir.files) {
                        fireCallbacks(f, WATCH_LOST);
                        filepathCallbacks.remove(f);
                    }
                }
            }
        }
    }

    /**
     * Dispatches notifications to callbacks registered for {@code absPath}.
     * {@code kind} is one of {@code ENTRY_CREATE}, {@code ENTRY_MODIFY}, or {@link #WATCH_LOST}.
     */
    private void fireCallbacks(final Path absPath, final WatchEvent.Kind<?> kind) {
        final CopyOnWriteArrayList<FileChangeCallback> callbacks = filepathCallbacks.get(absPath);
        if (callbacks == null) return;
        final FileChangeEvent evt = new FileChangeEvent(absPath, kind);
        for (final FileChangeCallback cb : callbacks) {
            try {
                cb.onChange(evt);
            } catch (final Exception e) {
                logger.warn("FileChangeCallback {} threw exception for path {}", cb.getClass().getName(), absPath, e);
            }
        }
    }
}
