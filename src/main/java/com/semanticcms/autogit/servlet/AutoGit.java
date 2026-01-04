/*
 * semanticcms-autogit-servlet - SemanticCMS automatic Git in a Servlet environment.
 * Copyright (C) 2016, 2018, 2020, 2021, 2022, 2024, 2025, 2026  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of semanticcms-autogit-servlet.
 *
 * semanticcms-autogit-servlet is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * semanticcms-autogit-servlet is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with semanticcms-autogit-servlet.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.semanticcms.autogit.servlet;

import com.aoapps.lang.ProcessResult;
import com.aoapps.lang.Strings;
import com.aoapps.servlet.attribute.ScopeEE;
import com.semanticcms.autogit.model.GitStatus;
import com.semanticcms.autogit.model.State;
import com.semanticcms.autogit.model.UncommittedChange;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.annotation.WebListener;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tracks the current git status.
 */
public class AutoGit {

  private static final boolean DEBUG = false; // false for production releases

  private static final ScopeEE.Application.Attribute<AutoGit> APPLICATION_ATTRIBUTE =
      ScopeEE.APPLICATION.attribute(AutoGit.class.getName());

  /**
   * The lock file used by Git that ignored for file changes.
   */
  private static final String GIT_LOCK_FILE = "index.lock";

  /**
   * The number of milliseconds between automatic "git pull".
   */
  private static final long GIT_PULL_MILLIS = 5L * 60L * 1000L; // 5 minutes

  /**
   * The number of milliseconds after a change is detected to begin the Git status update.
   */
  private static final long AFTER_CHANGE_DELAY = 1000;

  /**
   * The number of milliseconds to delay after an exception.
   */
  private static final long AFTER_EXCEPTION_DELAY = 10000;

  /**
   * The number of milliseconds that status not updated until considered "TIMEOUT".
   */
  private static final long TIMEOUT_MILLIS = GIT_PULL_MILLIS * 2;

  private static final String GIT_TOPLEVEL_CONTEXT_PARAM = "git.toplevel";

  /**
   * Manages automatic Git repository interactions.
   */
  @WebListener("Manages automatic Git repository interactions.")
  public static class Initializer implements ServletContextListener {

    private AutoGit instance;

    @Override
    public void contextInitialized(ServletContextEvent event) {
      try {
        ServletContext servletContext = event.getServletContext();
        instance = new AutoGit(servletContext);
        APPLICATION_ATTRIBUTE.context(servletContext).set(instance);
        instance.start();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
      APPLICATION_ATTRIBUTE.context(event.getServletContext()).remove();
      if (instance != null) {
        instance.stop();
        instance = null;
      }
    }
  }

  private final ServletContext servletContext;

  private final Path gitToplevel;

  private volatile WatchService watcher; // Set to null when stopped

  private final Map<Path, WatchKey> registered = new HashMap<>();

  private volatile Thread watcherThread; // Set to null when stopped

  private volatile Thread changedThread; // Set to null when stopped

  private AutoGit(ServletContext servletContext) throws IOException {
    this.servletContext = servletContext;
    // Find the top level directory
    String gitToplevelPath = Strings.trimNullIfEmpty(servletContext.getInitParameter(GIT_TOPLEVEL_CONTEXT_PARAM));
    File gitToplevelRaw;
    if (gitToplevelPath == null) {
      // Default to web root
      String rootRealPath = servletContext.getRealPath("/");
      if (rootRealPath == null) {
        throw new IllegalStateException("Unable to find web root and " + GIT_TOPLEVEL_CONTEXT_PARAM + " context parameter not provided");
      }
      gitToplevelRaw = new File(rootRealPath);
    } else {
      if (gitToplevelPath.startsWith("~/")) {
        gitToplevelRaw = new File(System.getProperty("user.home"), gitToplevelPath.substring(2));
      } else {
        gitToplevelRaw = new File(gitToplevelPath);
      }
    }
    if (DEBUG) {
      servletContext.log("gitToplevelRaw: " + gitToplevelRaw);
    }
    gitToplevel = gitToplevelRaw.getCanonicalFile().toPath();
    if (DEBUG) {
      servletContext.log("gitToplevel: " + gitToplevelPath);
    }
    // Make sure root exists and is readable
    if (!Files.isDirectory(gitToplevel, LinkOption.NOFOLLOW_LINKS)) {
      throw new IOException("Git toplevel is not a directory: " + gitToplevel);
    }
    if (!Files.isReadable(gitToplevel)) {
      throw new IOException("Unable to read Git toplevel directory: " + gitToplevel);
    }
  }

  private void start() throws IOException {
    // Recursively watch for any changes in the directory
    if (DEBUG) {
      servletContext.log("Starting watcher");
    }
    watcher = gitToplevel.getFileSystem().newWatchService();
    resync();
    if (DEBUG) {
      servletContext.log("Starting watchThread");
    }
    watcherThread = new Thread(watcherRunnable);
    watcherThread.start();
    if (DEBUG) {
      servletContext.log("Starting changeThread");
    }
    changedThread = new Thread(changedRunnable);
    changedThread.start();
  }

  private void stop() {
    Thread ct = changedThread;
    changedThread = null;
    if (ct != null) {
      if (DEBUG) {
        log("Shutting down changeThread");
      }
      ct.interrupt();
    }

    Thread wt = watcherThread;
    watcherThread = null;
    if (wt != null) {
      if (DEBUG) {
        log("Shutting down watchThread");
      }
      wt.interrupt();
    }

    WatchService w = watcher;
    watcher = null;
    if (w != null) {
      if (DEBUG) {
        log("Shutting down watcher");
      }
      try {
        w.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    synchronized (registered) {
      // for (Map.Entry<Path, WatchKey> entry : registered.entrySet()) {
      //   if (DEBUG) {
      //     log("Canceling watch key: " + entry.getKey());
      //   }
      //   entry.getValue().cancel();
      // }
      registered.clear();
    }
  }

  private void log(String message) {
    servletContext.log(message);
  }

  private void log(String message, Throwable t) {
    servletContext.log(message, t);
  }

  /**
   * Resyncs the entire directory recursively, registering and canceling any
   * discrepancies found.
   */
  private void resync() throws IOException {
    WatchService w = watcher;
    if (w != null) {
      synchronized (registered) {
        Set<Path> extraKeys = new HashSet<>(registered.keySet());
        resync(w, gitToplevel, extraKeys);
        for (Path extraKey : extraKeys) {
          if (DEBUG) {
            log("Canceling watch key: " + extraKey);
          }
          registered.remove(extraKey).cancel();
        }
      }
    }
  }

  private void resync(WatchService w, Path path, Set<Path> extraKeys) throws IOException {
    assert Thread.holdsLock(registered);
    if (
        Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
            // Skip .git directory "index.lock" files
            && !path.endsWith(GIT_LOCK_FILE)
    // && !path.endsWith(".git")
    ) {
      WatchKey key = registered.get(path);
      if (key == null) {
        if (DEBUG) {
          log("Registering watch key: " + path);
        }
        registered.put(
            path,
            path.register(
                w,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
            )
        );
      } else if (!key.isValid()) {
        if (DEBUG) {
          log("Replacing invalid watch key: " + path);
        }
        registered.put(
            path,
            path.register(
                w,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
            )
        );
      }
      extraKeys.remove(path);
      try (DirectoryStream<Path> files = Files.newDirectoryStream(path)) {
        for (Path file : files) {
          resync(w, file, extraKeys);
        }
      }
    }
  }

  /**
   * Flag set to true whenever a possible change is detected.
   */
  private static class ChangedLock {
    // Empty lock class to help heap profile
  }

  private final ChangedLock changedLock = new ChangedLock();
  private boolean changed;

  @SuppressWarnings({"SleepWhileInLoop", "UseSpecificCatch", "TooBroadCatch"})
  private final Runnable watcherRunnable = () -> {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // Stop when thread set to null
        if (watcherThread == null) {
          return;
        }
        // Stop when watcher set to null
        WatchService w = watcher;
        if (w == null) {
          return;
        }
        // Get next key
        WatchKey key;
        try {
          key = w.take();
        } catch (ClosedWatchServiceException e) {
          // Normal on shutdown
          return;
        } catch (InterruptedException e) {
          // Restore the interrupted status
          Thread.currentThread().interrupt();
          break;
        }
        boolean doResync = false;
        boolean setChanged = false;
        for (WatchEvent<?> event : key.pollEvents()) {
          WatchEvent.Kind<?> kind = event.kind();
          if (kind == StandardWatchEventKinds.OVERFLOW) {
            if (DEBUG) {
              log("Overflow");
            }
            // Reevaluates all recursively on overflow
            doResync = true;
            setChanged = true;
          } else if (
              kind == StandardWatchEventKinds.ENTRY_CREATE
                  || kind == StandardWatchEventKinds.ENTRY_DELETE
                  || kind == StandardWatchEventKinds.ENTRY_MODIFY
          ) {
            @SuppressWarnings("unchecked")
            WatchEvent<Path> ev = (WatchEvent<Path>) event;
            Path filename = ev.context();
            if (filename.endsWith(GIT_LOCK_FILE)) {
              if (DEBUG) {
                log(kind + ": " + filename + " (Skipping Git lock)");
              }
            } else {
              if (DEBUG) {
                log(kind + ": " + filename);
              }
              if (!doResync) {
                // TODO: Add/remove directories more efficiently given specific kind and paths instead of full resync
                doResync = true;
              }
              setChanged = true;
            }
          } else {
            throw new AssertionError("Unexpected kind: " + kind);
          }
        }
        if (doResync) {
          resync();
        }
        if (setChanged) {
          synchronized (changedLock) {
            changed = true;
            changedLock.notify(); // notifyAll() not needed: only a single thread waiting
          }
        }
        boolean valid = key.reset();
        if (!valid) {
          // Remove from registered?
        }
      } catch (ThreadDeath td) {
        throw td;
      } catch (Throwable t) {
        // Stop when thread set to null
        if (watcherThread == null) {
          return;
        }
        log(null, t);
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          log(null, e);
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
    }
  };

  @SuppressWarnings({"SleepWhileInLoop", "UseSpecificCatch", "TooBroadCatch"})
  private final Runnable changedRunnable = () -> {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // Stop when thread set to null
        if (changedThread == null) {
          return;
        }
        // Clear changed flag now before updating Git status so any change during status check will trigger
        // another change detection
        synchronized (changedLock) {
          changed = false;
        }
        // Do git now
        updateGitStatus();
        long waitEndTime = System.currentTimeMillis() + GIT_PULL_MILLIS;
        boolean changeReceived;
        synchronized (changedLock) {
          while (true) {
            // Stop when thread set to null
            if (changedThread == null || Thread.currentThread().isInterrupted()) {
              return;
            }
            // Check if changed
            if (changed) {
              changeReceived = true;
              break;
            } else {
              // Wait at most up to GIT_PULL_MILLIS until changed flag set
              long now = System.currentTimeMillis();
              long millisToWait = waitEndTime - now;
              if (millisToWait <= 0) {
                if (DEBUG) {
                  log("changeRunnable, no more waiting");
                }
                changeReceived = false;
                break;
              }
              if (millisToWait > GIT_PULL_MILLIS) {
                // System time reset into the past
                if (DEBUG) {
                  log("changeRunnable, system time reset into the past");
                }
                waitEndTime = now + GIT_PULL_MILLIS;
                millisToWait = GIT_PULL_MILLIS;
              }
              if (DEBUG) {
                log("changeRunnable, waiting up to " + millisToWait + " ms");
              }
              try {
                changedLock.wait(millisToWait);
              } catch (InterruptedException e) {
                // Restore the interrupted status
                Thread.currentThread().interrupt();
                // Stop when thread set to null
                if (changedThread == null) {
                  return;
                }
                log(null, e);
              }
            }
          }
        }
        if (changeReceived) {
          // Delay a bit to batch changes
          if (DEBUG) {
            log("changeRunnable, change received");
          }
          try {
            Thread.sleep(AFTER_CHANGE_DELAY);
          } catch (InterruptedException e) {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
          }
        }
      } catch (ThreadDeath td) {
        throw td;
      } catch (Throwable t) {
        // Stop when thread set to null
        if (changedThread == null) {
          return;
        }
        log(null, t);
        try {
          Thread.sleep(AFTER_EXCEPTION_DELAY);
        } catch (InterruptedException e) {
          log(null, e);
          // Restore the interrupted status
          Thread.currentThread().interrupt();
        }
      }
    }
  };

  private volatile GitStatus status = new GitStatus(System.currentTimeMillis(), State.STARTING, Collections.emptyList());

  /**
   * Updates the current Git status, will block on I/O.
   * Automatically does git pull and git push when there are no uncommitted changes.
   *
   * <p>Called on startup.
   * Also called when file change detected and the Git status should be updated.</p>
   *
   * <p>TODO: Add timeout within this method?</p>
   */
  private void updateGitStatus() throws IOException, ParseException {
    final long now = System.currentTimeMillis();
    // Get a list of modules including "" for main module
    List<String> modules;
    {
      if (DEBUG) {
        log("Finding modules");
      }
      ProcessBuilder pb =
          new ProcessBuilder("git", "submodule", "--quiet", "foreach", "--recursive", "echo \"$path\"")
              .directory(gitToplevel.toFile());
      Process p = pb.start();
      ProcessResult result = ProcessResult.getProcessResult(p);
      if (result.getExitVal() != 0) {
        throw new IOException("Unable to find submodules: " + result.getStderr());
      }
      List<String> submodules = Strings.splitLines(result.getStdout());
      if (DEBUG) {
        for (String submodule : submodules) {
          log("Got submodule: " + submodule);
        }
      }
      // Add the empty module list
      modules = new ArrayList<>(submodules.size() + 1);
      modules.addAll(submodules);
      modules.add("");
    }
    // Get the status of each module
    State state = State.SYNCHRONIZED;
    List<UncommittedChange> uncommittedChanges = new ArrayList<>();
    for (String module : modules) {
      if (DEBUG) {
        log("Getting status of module \"" + module + '"');
      }
      File workingDir;
      if (module.isEmpty()) {
        // Main module
        workingDir = gitToplevel.toFile();
      } else {
        // Submodule
        workingDir = new File(gitToplevel.toFile(), module);
      }
      ProcessBuilder pb =
          new ProcessBuilder("git", "status", "--porcelain", "-z")
              .directory(workingDir);
      Process p = pb.start();
      ProcessResult result = ProcessResult.getProcessResult(p);
      if (result.getExitVal() != 0) {
        throw new IOException("Unable to get status: " + result.getStderr());
      }
      // Split on NUL (ASCII 0)
      List<String> split = new ArrayList<>(Strings.split(result.getStdout(), (char) 0));
      if (!split.isEmpty()) {
        // Remove last empty part of split
        String last = split.remove(split.size() - 1);
        if (!last.isEmpty()) {
          throw new ParseException("Last element of split is not empty: " + last, 0);
        }
      }
      // if ((split.size() % 2) != 0) {
      //   throw new ParseException("Unexpected split size: " + split.size(), 0);
      // }
      int i = 0;
      while (i < split.size()) {
        char x;
        char y;
        String from;
        String to;
        {
          String first = split.get(i++);
          if (first.length() < 3) {
            throw new ParseException("split1 length too short: " + first.length(), 0);
          }
          x = first.charAt(0);
          y = first.charAt(1);
          if (first.charAt(2) != ' ') {
            throw new ParseException("Third character of split1 is not a space: " + first.charAt(2), 0);
          }
          if (x == 'R') {
            // Is rename, will have both to and from
            to = first.substring(3);
            from = split.get(i++);
          } else {
            // Will have from only from
            to = null;
            from = first.substring(3);
          }
        }
        if (DEBUG) {
          log("x = \"" + x + '"');
          log("y = \"" + y + '"');
          log("from = \"" + from + '"');
          log("to = \"" + to + '"');
        }
        UncommittedChange uncommittedChange = new UncommittedChange(x, y, module, from, to);
        uncommittedChanges.add(uncommittedChange);
        // Keep the highest state
        State meaningState = uncommittedChange.getMeaning().getState();
        if (meaningState.compareTo(state) > 0) {
          state = meaningState;
        }
      }
    }
    if (DEBUG) {
      log("state = " + state);
    }
    // TODO: auto-commit any modified tasklogs
    // TODO: git pull all modules always to stay in sync
    // TODO: git push all modules if state is synchronized
    status = new GitStatus(now, state, Collections.unmodifiableList(uncommittedChanges));
  }

  /**
   * Gets the current Git status, will not block.
   */
  public GitStatus getGitStatus() {
    long now = System.currentTimeMillis();
    GitStatus gs = status;
    // Timeout when status not updated recently enough
    long statusTime = gs.getStatusTime();
    long millisSince = now - statusTime;
    if (
        millisSince >= TIMEOUT_MILLIS
            || millisSince <= -TIMEOUT_MILLIS // Can happen when system time reset
    ) {
      return new GitStatus(statusTime, State.TIMEOUT, Collections.emptyList());
    }
    // Return most recently determined status
    return gs;
  }

  /**
   * Gets the current instance or {@code null} when disabled.
   */
  public static AutoGit getInstance(ServletContext sc) {
    return APPLICATION_ATTRIBUTE.context(sc).get();
  }

  private static final ScopeEE.Request.Attribute<GitStatus> GIT_STATUS_REQUEST_CACHE_KEY =
      ScopeEE.REQUEST.attribute(AutoGit.class.getName() + ".getGitStatus.cache");

  /**
   * Gets the current git status.
   */
  public static GitStatus getGitStatus(
      ServletContext servletContext,
      ServletRequest request
  ) {
    // Look for cached value
    return GIT_STATUS_REQUEST_CACHE_KEY.context(request).computeIfAbsent(name -> {
      AutoGit gitContext = getInstance(servletContext);
      if (gitContext == null) {
        return new GitStatus(System.currentTimeMillis(), State.DISABLED, Collections.emptyList());
      } else {
        return gitContext.getGitStatus();
      }
    });
  }
}
