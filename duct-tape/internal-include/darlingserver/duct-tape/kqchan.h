#ifndef _DARLINGSERVER_DUCT_TAPE_KQCHAN_H_
#define _DARLINGSERVER_DUCT_TAPE_KQCHAN_H_

#include <stdint.h>

#include <os/refcnt.h>
#include <sys/event.h>
#include <sys/eventvar.h>
#include <kern/waitq.h>
#include <libsimple/lock.h>

#include <darlingserver/duct-tape.h>

typedef struct dtape_task dtape_task_t;
typedef struct dtape_kqchan_mach_port dtape_kqchan_mach_port_t;

struct dtape_kqchan_mach_port {
	os_refcnt_t refcount;
	dtape_task_t* task;
	struct knote knote;
	dtape_kqchan_mach_port_notification_callback_f callback;
	void* context;
	// held while the callback runs and while notifications are disabled, so that disabling
	// waits for an in-flight callback (the owner frees `context` right after disabling)
	libsimple_lock_t callback_lock;
	thread_t waiter_thread;
	struct waitq* waitq;
	dtape_semaphore_t* waiter_death_semaphore;
	dtape_semaphore_t* waiter_read_semaphore;
};

#endif // _DARLINGSERVER_DUCT_TAPE_KQCHAN_H_
