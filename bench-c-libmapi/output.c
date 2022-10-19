#include "runner.h"

#include <stdlib.h>

#define BUFFER_SIZE (100 * 1024)


struct {
	pthread_t thread;
	pthread_mutex_t lock;
	pthread_cond_t cond;
	struct buffer *head;
	struct buffer **append_here;
	bool active;
	bool shutting_down;
} queue;


struct buffer {
	size_t len;
	struct buffer *next;
	char data[BUFFER_SIZE];
};


static struct buffer*
new_buffer(void)
{
	struct buffer *buf = malloc(sizeof(struct buffer));
	if (!buf)
		die_errno("malloc buffer");
	buf->len = 0;
	buf->next = NULL;
	return buf;
}


static void
shutdown_queue(void)
{
	pthread_mutex_lock(&queue.lock);
	while (1) {
		if (!queue.active)
			break;
		if (!queue.shutting_down) {
			queue.shutting_down = true;
			pthread_cond_broadcast(&queue.cond);
		}
		pthread_cond_wait(&queue.cond, &queue.lock);
	}
	pthread_mutex_unlock(&queue.lock);
}


static void
append_to_queue(struct buffer *buf)
{
	pthread_mutex_lock(&queue.lock);
	*queue.append_here = buf;
	queue.append_here = &buf->next;
	pthread_cond_broadcast(&queue.cond);
	pthread_mutex_unlock(&queue.lock);
}


static struct buffer *
take_from_queue(void)
{
	struct buffer *buf;
	pthread_mutex_lock(&queue.lock);
	while (1) {
		if (queue.shutting_down) {
			buf = NULL;
			break;
		}
		if (queue.head) {
			if (queue.append_here == &queue.head->next)
				queue.append_here = &queue.head;
			buf = queue.head;
			queue.head = buf->next;
			break;
		}
		pthread_cond_wait(&queue.cond, &queue.lock);
	}
	pthread_mutex_unlock(&queue.lock);
	return buf;
}


static void*
do_work(void *dummy)
{
	struct buffer *buf;
	while (((buf = take_from_queue()))) {
		char *pos = buf->data;
		char *end = pos + buf->len;
		while (pos < end) {
			size_t nwritten = fwrite(pos, 1, end - pos, stdout);
			if (nwritten == 0) {
				fprintf(stderr, "Unexpected end of stdout\n");
				exit(1);
			} else if (nwritten < 0) {
				die_errno("write to stdout");
			}
			pos += nwritten;
		}
		free(buf);
	}
	pthread_mutex_lock(&queue.lock);
	queue.active = false;
	pthread_cond_broadcast(&queue.cond);
	pthread_mutex_unlock(&queue.lock);

	return NULL;
}


///////////////////////////////////////////////////////////////////////////////
// Public functions:

void
init_output(void)
{
	pthread_mutex_init(&queue.lock, NULL);
	pthread_cond_init(&queue.cond, NULL);
	queue.head = NULL;
	queue.append_here = &queue.head;
	queue.active = true;
	queue.shutting_down = false;
	if (0 != pthread_create(&queue.thread, NULL, do_work, NULL))
		die_errno("cannot create output worker thread");
}


void
flush_output(struct buffer **buf_p)
{
	struct buffer *buf = *buf_p;
	*buf_p = NULL;
	if (buf)
		append_to_queue(buf);
}


void write_output(struct buffer **buf_p, long timestamp)
{
	size_t required = 64; // enough for two 10-digit longs and a comma and a newline

	struct buffer *buf = *buf_p;

	if (buf == NULL) {
		*buf_p = buf = new_buffer();
	} else if (BUFFER_SIZE - buf->len < required) {
		flush_output(buf_p);
		*buf_p = buf = new_buffer();
	}

	size_t avail = BUFFER_SIZE - buf->len;
	int n = snprintf(&buf->data[buf->len], avail, "%ld\n", timestamp);
	if (n <= 0 || n >= avail) {
		fprintf(stderr, "error formatting timestamp\n");
		exit(1);
	}
	buf->len += n;
}

void
finish_output(void)
{
	shutdown_queue();
	pthread_join(queue.thread, NULL);
}