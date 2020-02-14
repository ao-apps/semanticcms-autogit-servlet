/*
 * semanticcms-autogit-servlet - SemanticCMS automatic Git in a Servlet environment.
 * Copyright (C) 2016, 2018, 2020  AO Industries, Inc.
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
 * along with semanticcms-autogit-servlet.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.semanticcms.autogit.servlet;

import com.aoindustries.lang.ProcessResult;
import com.aoindustries.util.StringUtility;
import com.aoindustries.util.WrappedException;
import com.semanticcms.autogit.model.GitStatus;
import com.semanticcms.autogit.model.State;
import com.semanticcms.autogit.model.UncommittedChange;
import java.io.File;
import java.io.IOException;
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
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.ServletRequest;
import javax.servlet.annotation.WebListener;

@WebListener("Manages automatic Git repository interactions.")
public class AutoGitContextListener implements ServletContextListener {

	private static final boolean DEBUG = false; // false for production releases

	private static final String APPLICATION_SCOPE_KEY = AutoGitContextListener.class.getName();

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
	 * The number of milliseconds that status not updated until considered "TIMEOUT"
	 */
	private static final long TIMEOUT_MILLIS = GIT_PULL_MILLIS * 2;

	private static final String GIT_TOPLEVEL_CONTEXT_PARAM = "git.toplevel";

	private static class ServletContextLock {}
	private final ServletContextLock servletContextLock = new ServletContextLock();
	private ServletContext servletContext;

	private static class GitToplevelLock {}
	private final GitToplevelLock gitToplevelLock = new GitToplevelLock();
	private Path gitToplevel;

	private static class WatcherLock {}
	private final WatcherLock watcherLock = new WatcherLock();
	private WatchService watcher; // Will be set to null when context shutdown

	private final Map<Path,WatchKey> registered = new HashMap<>();

	private static class WatcherThreadLock {}
	private final WatcherThreadLock watcherThreadLock = new WatcherThreadLock();
	private Thread watcherThread; // Set to null when context shutdown

	private static class ChangedThreadLock {}
	private final ChangedThreadLock changedThreadLock = new ChangedThreadLock();
	private Thread changedThread; // Set to null when context shutdown

	@Override
	public void contextInitialized(ServletContextEvent sce) {
		try {
			ServletContext sc;
			synchronized(servletContextLock) {
				servletContext = sce.getServletContext();
				sc = servletContext;
			}
			// Find the top level directory
			String gitToplevelPath = sc.getInitParameter(GIT_TOPLEVEL_CONTEXT_PARAM);
			File gitToplevelRaw;
			if(gitToplevelPath == null || gitToplevelPath.isEmpty()) {
				// Default to web root
				String rootRealPath = sc.getRealPath("/");
				if(rootRealPath == null) throw new IllegalStateException("Unable to find web root and " + GIT_TOPLEVEL_CONTEXT_PARAM + " context parameter not provided");
				gitToplevelRaw = new File(rootRealPath);
			} else {
				if(gitToplevelPath.startsWith("~/")) {
					gitToplevelRaw = new File(System.getProperty("user.home"), gitToplevelPath.substring(2));
				} else {
					gitToplevelRaw = new File(gitToplevelPath);
				}
			}
			if(DEBUG) sc.log("gitToplevelRaw: " + gitToplevelRaw);
			Path gtl;
			synchronized(gitToplevelLock) {
				gitToplevel = gitToplevelRaw.getCanonicalFile().toPath();
				gtl = gitToplevel;
			}
			if(DEBUG) sc.log("gitToplevel: " + gtl);
			// Make sure root exists and is readable
			if(!Files.isDirectory(gtl, LinkOption.NOFOLLOW_LINKS)) throw new IOException("Git toplevel is not a directory: " + gtl);
			if(!Files.isReadable(gtl)) throw new IOException("Unable to read Git toplevel directory: " + gtl);
			// Recursively watch for any changes in the directory
			if(DEBUG) sc.log("Starting watcher");
			WatchService w;
			synchronized(watcherLock) {
				watcher = gtl.getFileSystem().newWatchService();
				w = watcher;
			}
			resync();
			if(DEBUG) sc.log("Starting watchThread");
			synchronized(watcherThreadLock) {
				watcherThread = new Thread(watcherRunnable);
				watcherThread.start();
			}
			if(DEBUG) sc.log("Starting changeThread");
			synchronized(changedThreadLock) {
				changedThread = new Thread(changedRunnable);
				changedThread.start();
			}
			sc.setAttribute(APPLICATION_SCOPE_KEY, this);
		} catch(IOException e) {
			throw new WrappedException(e);
		}
	}

	private void log(String message) {
		ServletContext sc;
		synchronized(servletContextLock) {
			sc = servletContext;
		}
		if(sc != null) sc.log(message);
	}

	private void log(String message, Throwable t) {
		ServletContext sc;
		synchronized(servletContextLock) {
			sc = servletContext;
		}
		if(sc != null) sc.log(message, t);
	}

	/**
	 * Resyncs the entire directory recursively, registering and canceling any
	 * discrepancies found.
	 */
	private void resync() throws IOException {
		Path gtl;
		synchronized(gitToplevelLock) {
			gtl = gitToplevel;
		}
		WatchService w;
		synchronized(watcherLock) {
			w = watcher;
		}
		if(gtl != null && w != null) {
			synchronized(registered) {
				Set<Path> extraKeys = new HashSet<>(registered.keySet());
				resync(w, gtl, extraKeys);
				for(Path extraKey : extraKeys) {
					if(DEBUG) log("Canceling watch key: " + extraKey);
					registered.remove(extraKey).cancel();
				}
			}
		}
	}

	private void resync(WatchService w, Path path, Set<Path> extraKeys) throws IOException {
		assert Thread.holdsLock(registered);
		if(
			Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
			// Skip .git directory "index.lock" files
			&& !path.endsWith(GIT_LOCK_FILE)
			//&& !path.endsWith(".git")
		) {
			WatchKey key = registered.get(path);
			if(key == null) {
				if(DEBUG) log("Registering watch key: " + path);
				registered.put(
					path,
					path.register(
						w,
						StandardWatchEventKinds.ENTRY_CREATE,
						StandardWatchEventKinds.ENTRY_DELETE,
						StandardWatchEventKinds.ENTRY_MODIFY
					)
				);
			} else if(!key.isValid()) {
				if(DEBUG) log("Replacing invalid watch key: " + path);
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
				for(Path file : files) {
					resync(w, file, extraKeys);
				}
			}
		}
	}

	/** Flag set to true whenever a possible change is detected */
	private static class ChangedLock {}
	private final ChangedLock changedLock = new ChangedLock();
	private boolean changed = false;

	// Java 1.8: Use Lambda
	private final Runnable watcherRunnable = new Runnable() {
		@Override
		public void run() {
			while(true) {
				try {
					// Stop when thread set to null
					synchronized(watcherThreadLock) {
						if(watcherThread == null) return;
					}
					// Stop when watcher set to null
					WatchService w;
					synchronized(watcherLock) {
						w = watcher;
					}
					if(w == null) return;
					// Get next key
					WatchKey key;
					try {
						key = w.take();
					} catch(ClosedWatchServiceException e) {
						// Normal on shutdown
						return;
					} catch(InterruptedException e) {
						// Repeat loop
						continue;
					}
					boolean doResync = false;
					boolean setChanged = false;
					for(WatchEvent<?> event : key.pollEvents()) {
						WatchEvent.Kind<?> kind = event.kind();
						if(kind == StandardWatchEventKinds.OVERFLOW) {
							if(DEBUG) log("Overflow");
							// Reevaluates all recursively on overflow
							doResync = true;
							setChanged = true;
						} else if(
							kind == StandardWatchEventKinds.ENTRY_CREATE
							|| kind == StandardWatchEventKinds.ENTRY_DELETE
							|| kind == StandardWatchEventKinds.ENTRY_MODIFY
						) {
							@SuppressWarnings("unchecked")
							WatchEvent<Path> ev = (WatchEvent<Path>)event;
							Path filename = ev.context();
							if(filename.endsWith(GIT_LOCK_FILE)) {
								if(DEBUG) log(kind + ": " + filename + " (Skipping Git lock)");
							} else {
								if(DEBUG) log(kind + ": " + filename);
								if(!doResync) {
									// TODO: Add/remove directories more efficiently given specific kind and paths instead of full resync
									doResync = true;
								}
								setChanged = true;
							}
						} else {
							throw new AssertionError("Unexpected kind: " + kind);
						}
					}
					if(doResync) {
						resync();
					}
					if(setChanged) {
						synchronized(changedLock) {
							changed = true;
							changedLock.notify();
						}
					}
					boolean valid = key.reset();
					if(!valid) {
						// Remove from registered?
					}
				} catch(ThreadDeath td) {
					throw td;
				} catch(Throwable t) {
					log(null, t);
					try {
						Thread.sleep(10000);
					} catch(InterruptedException e) {
						// Continue loop
					}
				}
			}
		}
	};

	// Java 1.8: Use Lambda
	private final Runnable changedRunnable = new Runnable() {
		@Override
		public void run() {
			while(true) {
				try {
					// Stop when thread set to null
					synchronized(changedThreadLock) {
						if(changedThread == null) return;
					}
					// Clear changed flag now before updating Git status so any change during status check will trigger
					// another change detection
					synchronized(changedLock) {
						changed = false;
					}
					// Do git now
					updateGitStatus();
					long waitEndTime = System.currentTimeMillis() + GIT_PULL_MILLIS;
					boolean changeReceived;
					synchronized(changedLock) {
						while(true) {
							// Stop when thread set to null
							synchronized(changedThreadLock) {
								if(changedThread == null) return;
							}
							// Check if changed
							if(changed) {
								changeReceived = true;
								break;
							} else {
								// Wait at most up to GIT_PULL_MILLIS until changed flag set
								long now = System.currentTimeMillis();
								long millisToWait = waitEndTime - now;
								if(millisToWait <= 0) {
									if(DEBUG) log("changeRunnable, no more waiting");
									changeReceived = false;
									break;
								}
								if(millisToWait > GIT_PULL_MILLIS) {
									// System time reset into the past
									if(DEBUG) log("changeRunnable, system time reset into the past");
									waitEndTime = now + GIT_PULL_MILLIS;
									millisToWait = GIT_PULL_MILLIS;
								}
								if(DEBUG) log("changeRunnable, waiting up to " + millisToWait + " ms");
								try {
									changedLock.wait(millisToWait);
								} catch(InterruptedException e) {
									// Normal on shutdown
								}
							}
						}
					}
					if(changeReceived) {
						// Delay a bit to batch changes
						if(DEBUG) log("changeRunnable, change received");
						try {
							Thread.sleep(AFTER_CHANGE_DELAY);
						} catch(InterruptedException e) {
							// Continue on
						}
					}
				} catch(ThreadDeath td) {
					throw td;
				} catch(Throwable t) {
					log(null, t);
					try {
						Thread.sleep(AFTER_EXCEPTION_DELAY);
					} catch(InterruptedException e) {
						// Continue loop
					}
				}
			}
		}
	};

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		ServletContext sc = sce.getServletContext();
		sc.removeAttribute(APPLICATION_SCOPE_KEY);

		Thread ct;
		synchronized(changedThreadLock) {
			ct = changedThread;
			changedThread = null;
		}
		if(ct != null) {
			if(DEBUG) log("Shutting down changeThread");
			ct.interrupt();
		}

		Thread wt;
		synchronized(watcherThreadLock) {
			wt = watcherThread;
			watcherThread = null;
		}
		if(wt != null) {
			if(DEBUG) log("Shutting down watchThread");
			wt.interrupt();
		}

		synchronized(watcherLock) {
			if(watcher != null) {
				if(DEBUG) log("Shutting down watcher");
				try {
					watcher.close();
				} catch(IOException e) {
					throw new WrappedException(e);
				}
				watcher = null;
			}
		}

		synchronized(registered) {
			/*
			for(Map.Entry<Path,WatchKey> entry : registered.entrySet()) {
				if(DEBUG) log("Canceling watch key: " + entry.getKey());
				entry.getValue().cancel();
			}*/
			registered.clear();
		}

		synchronized(gitToplevelLock) {
			gitToplevel = null;
		}

		synchronized(servletContextLock) {
			servletContext = null;
		}
	}

	private static class StatusLock {}
	private final StatusLock statusLock = new StatusLock();
	private GitStatus status;
	{
		// Java 1.8: Inline this
		List<UncommittedChange> emptyList = Collections.emptyList();
		status = new GitStatus(System.currentTimeMillis(), State.STARTING, emptyList);
	}

	/**
	 * Updates the current Git status, will block on I/O.
	 * Automatically does git pull and git push when there are no uncommitted changes.
	 * <p>
	 * Called on startup.
	 * Also called when file change detected and the Git status should be updated.
	 * </p>
	 * TODO: Add timeout within this method?
	 */
	private void updateGitStatus() throws IOException, ParseException {
		long now = System.currentTimeMillis();
		Path gtl;
		synchronized(gitToplevelLock) {
			gtl = gitToplevel;
		}
		GitStatus newStatus;
		if(gtl == null) {
			// Java 1.8: Inline this
			List<UncommittedChange> emptyList = Collections.emptyList();
			newStatus = new GitStatus(now, State.DISABLED, emptyList);
		} else {
			// Get a list of modules including "" for main module
			List<String> modules;
			{
				if(DEBUG) log("Finding modules");
				ProcessBuilder pb =
					new ProcessBuilder("git", "submodule", "--quiet", "foreach", "--recursive", "echo \"$path\"")
					.directory(gtl.toFile())
				;
				Process p = pb.start();
				ProcessResult result = ProcessResult.getProcessResult(p);
				if(result.getExitVal() != 0) throw new IOException("Unable to find submodules: " + result.getStderr());
				List<String> submodules = StringUtility.splitLines(result.getStdout());
				if(DEBUG) {
					for(String submodule : submodules) log("Got submodule: " + submodule);
				}
				// Add the empty module list
				modules = new ArrayList<>(submodules.size() + 1);
				modules.addAll(submodules);
				modules.add("");
			}
			// Get the status of each module
			State state = State.SYNCHRONIZED;
			List<UncommittedChange> uncommittedChanges = new ArrayList<>();
			for(String module : modules) {
				if(DEBUG) log("Getting status of module \"" + module + '"');
				File workingDir;
				if(module.isEmpty()) {
					// Main module
					workingDir = gtl.toFile();
				} else {
					// Submodule
					workingDir = new File(gtl.toFile(), module);
				}
				ProcessBuilder pb =
					new ProcessBuilder("git", "status", "--porcelain", "-z")
					.directory(workingDir)
				;
				Process p = pb.start();
				ProcessResult result = ProcessResult.getProcessResult(p);
				if(result.getExitVal() != 0) throw new IOException("Unable to get status: " + result.getStderr());
				// Split on NUL (ASCII 0)
				List<String> split = new ArrayList<>(StringUtility.splitString(result.getStdout(), (char)0));
				if(!split.isEmpty()) {
					// Remove last empty part of split
					String last = split.remove(split.size()-1);
					if(!last.isEmpty()) throw new ParseException("Last element of split is not empty: " + last, 0);
				}
				//if((split.size() % 2) != 0) throw new ParseException("Unexpected split size: " + split.size(), 0);
				int i = 0;
				while(i < split.size()) {
					char x;
					char y;
					String from;
					String to;
					{
						String first = split.get(i++);
						if(first.length() < 3) throw new ParseException("split1 length too short: " + first.length(), 0);
						x = first.charAt(0);
						y = first.charAt(1);
						if(first.charAt(2) != ' ') throw new ParseException("Third character of split1 is not a space: " + first.charAt(2), 0);
						if(x == 'R') {
							// Is rename, will have both to and from
							to = first.substring(3);
							from = split.get(i++);
						} else {
							// Will have from only from
							to = null;
							from = first.substring(3);
						}
					}
					if(DEBUG) {
						log("x = \"" + x + '"');
						log("y = \"" + y + '"');
						log("from = \"" + from + '"');
						log("to = \"" + to + '"');
					}
					UncommittedChange uncommittedChange = new UncommittedChange(x, y, module, from, to);
					uncommittedChanges.add(uncommittedChange);
					// Keep the highest state
					State meaningState = uncommittedChange.getMeaning().getState();
					if(meaningState.compareTo(state) > 0) state = meaningState;
				}
			}
			if(DEBUG) log("state = " + state);
			// TODO: auto-commit any modified tasklogs
			// TODO: git pull all modules always to stay in sync
			// TODO: git push all modules if state is synchronized
			newStatus = new GitStatus(now, state, Collections.unmodifiableList(uncommittedChanges));
		}
		synchronized(statusLock) {
			status = newStatus;
		}
	}

	/**
	 * Gets the current Git status, will not block.
	 */
	public GitStatus getGitStatus() {
		long now = System.currentTimeMillis();
		synchronized(statusLock) {
			// Timeout when status not updated recently enough
			long statusTime = status.getStatusTime();
			long millisSince = now - statusTime;
			if(
				millisSince >= TIMEOUT_MILLIS
				|| millisSince <= -TIMEOUT_MILLIS // Can happen when system time reset
			) {
				// Java 1.8: Inline this
				List<UncommittedChange> emptyList = Collections.emptyList();
				return new GitStatus(statusTime, State.TIMEOUT, emptyList);
			}
			// Return most recently determined status
			return status;
		}
	}

	public static AutoGitContextListener getInstance(ServletContext sc) {
		return (AutoGitContextListener)sc.getAttribute(APPLICATION_SCOPE_KEY);
	}

	private static final String GIT_STATUS_REQUEST_CACHE_KEY = AutoGitContextListener.class.getName() + ".getGitStatus.cache";

	public static GitStatus getGitStatus(
		ServletContext servletContext,
		ServletRequest request
	) {
		// Look for cached value
		GitStatus gitStatus = (GitStatus)request.getAttribute(GIT_STATUS_REQUEST_CACHE_KEY);
		if(gitStatus == null) {
			AutoGitContextListener gitContext = AutoGitContextListener.getInstance(servletContext);
			if(gitContext == null) {
				// Java 1.8: Inline this
				List<UncommittedChange> emptyList = Collections.emptyList();
				gitStatus = new GitStatus(System.currentTimeMillis(), State.DISABLED, emptyList);
			} else {
				gitStatus = gitContext.getGitStatus();
			}
			request.setAttribute(GIT_STATUS_REQUEST_CACHE_KEY, gitStatus);
		}
		return gitStatus;
	}
}
