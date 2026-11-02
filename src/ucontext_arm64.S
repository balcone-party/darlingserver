// getcontext/setcontext/swapcontext for microthreads on glibc aarch64 without the signal mask:
// glibc's versions do rt_sigprocmask on every switch (several per server call). Host signals
// never matter for a switch here: the server handles SIGUSR1 only and switches outside handlers.
// Layout is glibc's ucontext_t: uc_mcontext.regs at 184, sp 432, pc 440, __reserved 464 (an
// fpsimd_context: magic, size, fpsr, fpcr, vregs[32] at 480). Callee-saved state only, plus x0
// cleared so a resumed getcontext returns 0; setcontext loads x0-x30 for makecontext's arguments.

	.text
	.p2align 2
	.globl dserver_getcontext
	.type dserver_getcontext, %function
dserver_getcontext:
	bti c
	stp x19, x20, [x0, #336]
	stp x21, x22, [x0, #352]
	stp x23, x24, [x0, #368]
	stp x25, x26, [x0, #384]
	stp x27, x28, [x0, #400]
	stp x29, x30, [x0, #416]
	str xzr, [x0, #184]
	mov x9, sp
	str x9, [x0, #432]
	str x30, [x0, #440]
	movz w9, #0x8001
	movk w9, #0x4650, lsl #16
	str w9, [x0, #464]
	mov w9, #528
	str w9, [x0, #468]
	mrs x9, fpsr
	str w9, [x0, #472]
	mrs x9, fpcr
	str w9, [x0, #476]
	str d8, [x0, #608]
	str d9, [x0, #624]
	str d10, [x0, #640]
	str d11, [x0, #656]
	str d12, [x0, #672]
	str d13, [x0, #688]
	str d14, [x0, #704]
	str d15, [x0, #720]
	mov x0, #0
	ret
	.size dserver_getcontext, . - dserver_getcontext

	.p2align 2
	.globl dserver_setcontext
	.type dserver_setcontext, %function
dserver_setcontext:
	bti c
	ldr d8, [x0, #608]
	ldr d9, [x0, #624]
	ldr d10, [x0, #640]
	ldr d11, [x0, #656]
	ldr d12, [x0, #672]
	ldr d13, [x0, #688]
	ldr d14, [x0, #704]
	ldr d15, [x0, #720]
	ldr x9, [x0, #432]
	mov sp, x9
	ldr x16, [x0, #440]
	ldp x2, x3, [x0, #200]
	ldp x4, x5, [x0, #216]
	ldp x6, x7, [x0, #232]
	ldp x8, x9, [x0, #248]
	ldp x10, x11, [x0, #264]
	ldp x12, x13, [x0, #280]
	ldp x14, x15, [x0, #296]
	ldp x18, x19, [x0, #328]
	ldp x20, x21, [x0, #344]
	ldp x22, x23, [x0, #360]
	ldp x24, x25, [x0, #376]
	ldp x26, x27, [x0, #392]
	ldp x28, x29, [x0, #408]
	ldr x30, [x0, #424]
	ldr x1, [x0, #192]
	ldr x0, [x0, #184]
	ret x16
	.size dserver_setcontext, . - dserver_setcontext

	.p2align 2
	.globl dserver_swapcontext
	.type dserver_swapcontext, %function
dserver_swapcontext:
	bti c
	stp x19, x20, [x0, #336]
	stp x21, x22, [x0, #352]
	stp x23, x24, [x0, #368]
	stp x25, x26, [x0, #384]
	stp x27, x28, [x0, #400]
	stp x29, x30, [x0, #416]
	str xzr, [x0, #184]
	mov x9, sp
	str x9, [x0, #432]
	str x30, [x0, #440]
	movz w9, #0x8001
	movk w9, #0x4650, lsl #16
	str w9, [x0, #464]
	mov w9, #528
	str w9, [x0, #468]
	str d8, [x0, #608]
	str d9, [x0, #624]
	str d10, [x0, #640]
	str d11, [x0, #656]
	str d12, [x0, #672]
	str d13, [x0, #688]
	str d14, [x0, #704]
	str d15, [x0, #720]
	mov x0, x1
	b dserver_setcontext
	.size dserver_swapcontext, . - dserver_swapcontext

	.section .note.GNU-stack, "", %progbits
