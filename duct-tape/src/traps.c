#include <darlingserver/duct-tape.h>

#include <mach/mach_traps.h>
#include <kern/misc_protos.h>
#include <darlingserver/duct-tape/log.h>
#include <darlingserver/duct-tape/task.h>

uint32_t dtape_task_self_trap(void) {
	return task_self_trap(NULL);
};

uint32_t dtape_host_self_trap(void) {
	return host_self_trap(NULL);
};

uint32_t dtape_thread_self_trap(void) {
	return thread_self_trap(NULL);
};

uint32_t dtape_mach_reply_port(void) {
	return mach_reply_port(NULL);
};

uint32_t dtape_thread_get_special_reply_port(void) {
	return thread_get_special_reply_port(NULL);
};

uint32_t dtape_mk_timer_create(void) {
	return mk_timer_create_trap(NULL);
};

// iosc: deferred port operations, see iosc_port_op_t. The array is read from the caller's memory
// (normally its shared window) and is untrusted: a bad kind or count fails the batch, never more.
static uint64_t ops_failed;

static int caller_pid(void) {
	return (int)dtape_task_for_xnu_task(current_task())->saved_pid;
}

static void run_port_ops(uint64_t ops, uint32_t count) {
	if (!ops || !count) {
		return;
	}
	if (count > IOSC_PORT_OPS_MAX) {
		dtape_log_error("pid %d: port ops: count %u", caller_pid(), count);
		return;
	}
	iosc_port_op_t buf[IOSC_PORT_OPS_MAX];
	if (copyin(ops, buf, sizeof(buf[0]) * count)) {
		dtape_log_error("pid %d: port ops: copyin failed", caller_pid());
		return;
	}
	mach_port_name_t self = task_self_trap(NULL);
	for (uint32_t i = 0; i < count; i++) {
		const iosc_port_op_t* op = &buf[i];
		kern_return_t kr;
		switch (op->kind) {
			case iosc_port_op_deallocate: {
				struct _kernelrpc_mach_port_deallocate_args a = { .target = self, .name = op->name };
				kr = _kernelrpc_mach_port_deallocate_trap(&a);
				break;
			}
			case iosc_port_op_mod_refs: {
				struct _kernelrpc_mach_port_mod_refs_args a = { .target = self, .name = op->name, .right = op->arg, .delta = op->delta };
				kr = _kernelrpc_mach_port_mod_refs_trap(&a);
				break;
			}
			case iosc_port_op_insert_member: {
				struct _kernelrpc_mach_port_insert_member_args a = { .target = self, .name = op->name, .pset = op->arg };
				kr = _kernelrpc_mach_port_insert_member_trap(&a);
				break;
			}
			case iosc_port_op_extract_member: {
				struct _kernelrpc_mach_port_extract_member_args a = { .target = self, .name = op->name, .pset = op->arg };
				kr = _kernelrpc_mach_port_extract_member_trap(&a);
				break;
			}
			default:
				kr = KERN_INVALID_ARGUMENT;
				break;
		}
		if (kr != KERN_SUCCESS) {
			uint64_t n = __atomic_add_fetch(&ops_failed, 1, __ATOMIC_RELAXED);
			dtape_log_error("pid %d: deferred port op %u (name %#x arg %#x delta %d) failed: %d (%llu so far)",
				caller_pid(), op->kind, op->name, op->arg, op->delta, kr, (unsigned long long)n);
		}
	}
	// task_self_trap added a send reference
	struct _kernelrpc_mach_port_deallocate_args d = { .target = self, .name = self };
	_kernelrpc_mach_port_deallocate_trap(&d);
};

void dtape_run_port_ops(uint64_t ops, uint32_t ops_count) {
	run_port_ops(ops, ops_count);
};

// iosc: the receive behind a lane's SERVER entry; the message is queued already, so no waiting
int dtape_lane_take(uint64_t msg, uint32_t rcv_size, uint32_t rcv_name, int32_t option) {
	struct mach_msg_overwrite_trap_args args = {
		.msg = msg,
		.option = (option & ~(MACH_SEND_MSG | MACH_RCV_TIMEOUT)) | MACH_RCV_MSG | MACH_RCV_TIMEOUT,
		.send_size = 0,
		.rcv_size = rcv_size,
		.rcv_name = rcv_name,
		.timeout = 0,
		.priority = 0,
		.rcv_msg = 0,
	};
	current_thread()->iosc_lane_take = 1;
	int r = mach_msg_overwrite_trap(&args);
	current_thread()->iosc_lane_take = 0;
	return r;
};

DSERVER_DTAPE_DEFS;
