package com.monetdbsolutions.clientbench;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ResultWriter implements AutoCloseable {
	final Lock lock;
	final Condition cond;
	final ArrayDeque<Batch> queue;
	final Thread worker;
	final PrintStream printer;
	boolean shutdown;

	public ResultWriter(OutputStream out) {
		this(new BufferedOutputStream(out));
	}

	public ResultWriter(BufferedOutputStream out) {
		lock = new ReentrantLock();
		cond = lock.newCondition();
		queue = new ArrayDeque<>(1000);
		worker = new Thread(this::work);
		printer = new PrintStream(out, false);
		worker.start();
	}

	public Submitter newSubmitter() {
		return new Submitter();
	}

	private void submit(Batch batch) {
		lock.lock();
		try {
			queue.add(batch);
			cond.signal();
		} finally {
			lock.unlock();
		}
	}

	private Batch nextBatch() throws InterruptedException {
		lock.lock();
		try {
			while (true) {
				if (Thread.currentThread().isInterrupted()) {
					throw new InterruptedException();
				}
				Batch batch = queue.poll();
				if (batch != null) {
					return batch;
				}
				if (shutdown) {
					return null;
				}
				cond.await();
			}
		} finally {
			lock.unlock();
		}
	}

	private void work() {
		while (true) {
			Batch batch;
			try {
				batch = nextBatch();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}
			if (batch == null) {
				break;
			}
			for (int i = 0; i < batch.n; i++) {
				long nanos = batch.durations[i];
				printer.println(nanos);
			}
		}

		printer.flush();
	}

	@Override
	public void close() throws InterruptedException {
		lock.lock();
		try {
			shutdown = true;
			cond.signal();
		} finally {
			lock.unlock();
		}
		worker.join();
	}

	private static class Batch {
		final long[] durations;
		int n;

		Batch() {
			n = 0;
			durations = new long[1000];
		}

		boolean isFull() {
			return n == durations.length;
		}

		public void add(long nanos) {
			durations[n] = nanos;
			n++;
		}
	}

	public class Submitter implements AutoCloseable {
		private Batch batch;

		Submitter() {
			batch = new Batch();
		}

		public void submit(long nanos) {
			batch.add(nanos);
			if (batch.isFull()) {
				flush();
			}
		}

		public void flush() {
			Batch b = this.batch;
			this.batch = new Batch();
			ResultWriter.this.submit(b);
		}

		@Override
		public void close() {
			flush();
		}
	}
}
