//===----------------------------------------------------------------------===//
// delta_kernel_sdk.hpp — kernel-shipped RAII C++ adapters over the DuckDB scan C ABI.
//
// The kernel exposes the plan-based scan as a steppable state machine through a flat C ABI
// (`kdf_*` in the generated `delta_kernel_ffi.hpp`, namespace `ffi`). That ABI trades in raw
// owning pointers + malloc'd error strings, so every call site has to remember which pointer to
// free with which `kdf_*_free` and how to turn an `out_err` into an exception. This header pulls
// that bookkeeping down into the kernel-owned SDK: typed, move-only RAII wrappers that free on
// scope exit and convert `out_err` into a thrown `KernelException`. The engine drives the SM
// through these instead of touching the C ABI directly.
//
// Header-only, exceptions-based, no dependencies beyond the generated FFI header and the C++
// standard library. `#define DELTA_KERNEL_SDK_NO_EXCEPTIONS` before including to get the
// error-code forms only (see `try_*` methods) if the engine forbids exceptions at a boundary.
//
// Ownership contract mirrors the ABI docs:
//   - `KdfSM*`     from kdf_scan_open           -> ScanStateMachine (kdf_sm_free)
//   - `char*`      from kdf_sm_*_sql             -> KernelString     (kdf_string_free)
//   - `uint8_t*`   from kdf_sm_*_plan            -> KernelBytes      (kdf_bytes_free)
//   - `char*`      from any out_err              -> KernelException  (kdf_string_free, then throw)
//===----------------------------------------------------------------------===//
#pragma once

// The generated C ABI header. Defaults to the kernel-shipped `delta_kernel_ffi.hpp`; an engine that
// consumes a locally-patched copy (e.g. DuckDB post-processes cbindgen output into
// `generated_delta_kernel_ffi.hpp`) defines DELTA_KERNEL_SDK_FFI_HEADER to point at its own name
// before including this. Either way it provides namespace `ffi` with kdf_*, KdfSM,
// FFI_ArrowArray/Schema, EnginePredicate, and the KDF_STEP_* constants.
#ifndef DELTA_KERNEL_SDK_FFI_HEADER
#define DELTA_KERNEL_SDK_FFI_HEADER "delta_kernel_ffi.hpp"
#endif
#include DELTA_KERNEL_SDK_FFI_HEADER

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>

namespace delta_kernel {
namespace sdk {

//===----------------------------------------------------------------------===//
// Errors
//===----------------------------------------------------------------------===//

//! Thrown when a `kdf_*` call reports an error. Carries the operation name and the kernel's message.
class KernelException : public std::runtime_error {
public:
	explicit KernelException(const std::string &msg) : std::runtime_error(msg) {}
};

namespace detail {

//! Turn an `out_err` slot into a thrown `KernelException`. Frees the kernel string. `err` may be
//! null (then a generic message is used). Never returns.
[[noreturn]] inline void ThrowKernelError(const char *what, char *err) {
	std::string msg = err ? std::string(err) : std::string("unknown error");
	if (err) {
		ffi::kdf_string_free(err);
	}
	throw KernelException(std::string(what) + " failed: " + msg);
}

} // namespace detail

//===----------------------------------------------------------------------===//
// Owned C-string returned by the `_sql` ABI (freed with kdf_string_free)
//===----------------------------------------------------------------------===//

//! Move-only owner of a `char*` from a `kdf_sm_*_sql` call. Frees with `kdf_string_free` on scope
//! exit. Convertible to `std::string` (copies) or borrowable as a `const char*`.
class KernelString {
public:
	KernelString() = default;
	explicit KernelString(char *owned) : ptr_(owned) {}
	~KernelString() { reset(); }

	KernelString(KernelString &&o) noexcept : ptr_(o.ptr_) { o.ptr_ = nullptr; }
	KernelString &operator=(KernelString &&o) noexcept {
		if (this != &o) {
			reset();
			ptr_ = o.ptr_;
			o.ptr_ = nullptr;
		}
		return *this;
	}
	KernelString(const KernelString &) = delete;
	KernelString &operator=(const KernelString &) = delete;

	const char *c_str() const { return ptr_; }
	bool empty() const { return ptr_ == nullptr; }
	std::string str() const { return ptr_ ? std::string(ptr_) : std::string(); }

private:
	void reset() {
		if (ptr_) {
			ffi::kdf_string_free(ptr_);
			ptr_ = nullptr;
		}
	}
	char *ptr_ = nullptr;
};

//===----------------------------------------------------------------------===//
// Owned byte buffer returned by the `_plan` (proto) ABI (freed with kdf_bytes_free)
//===----------------------------------------------------------------------===//

//! Move-only owner of a `(uint8_t*, len)` proto buffer from a `kdf_sm_*_plan` call. Frees with
//! `kdf_bytes_free` on scope exit. Borrow the bytes via `data()`/`size()` (e.g. to feed
//! `google::protobuf::MessageLite::ParseFromArray`). The kernel is the single source of the
//! generated proto structs, so the engine decodes these with the kernel-shipped `.pb.h`.
class KernelBytes {
public:
	KernelBytes() = default;
	KernelBytes(uint8_t *owned, size_t len) : ptr_(owned), len_(len) {}
	~KernelBytes() { reset(); }

	KernelBytes(KernelBytes &&o) noexcept : ptr_(o.ptr_), len_(o.len_) {
		o.ptr_ = nullptr;
		o.len_ = 0;
	}
	KernelBytes &operator=(KernelBytes &&o) noexcept {
		if (this != &o) {
			reset();
			ptr_ = o.ptr_;
			len_ = o.len_;
			o.ptr_ = nullptr;
			o.len_ = 0;
		}
		return *this;
	}
	KernelBytes(const KernelBytes &) = delete;
	KernelBytes &operator=(const KernelBytes &) = delete;

	const uint8_t *data() const { return ptr_; }
	size_t size() const { return len_; }
	bool empty() const { return ptr_ == nullptr || len_ == 0; }

private:
	void reset() {
		if (ptr_) {
			ffi::kdf_bytes_free(ptr_, len_);
			ptr_ = nullptr;
			len_ = 0;
		}
	}
	uint8_t *ptr_ = nullptr;
	size_t len_ = 0;
};

//===----------------------------------------------------------------------===//
// The scan state machine
//===----------------------------------------------------------------------===//

//! What `ScanStateMachine::GetStep()` returned: the kernel needs a Reduce run, or the SM is done.
enum class Step {
	Reduce, //! KDF_STEP_REDUCE: fetch the pending reduce (SQL or plan), run it, SubmitReduce the Arrow.
	Done,   //! KDF_STEP_DONE: fetch the terminal result (SQL or plan) and execute the scan.
};

//! Move-only RAII owner of a `KdfSM*`. Opens (`Open`), drives (`GetStep`/`Submit*`), and yields the
//! terminal plan (`Result*`). The kernel is passive: DuckDB owns the loop and calls these; no engine
//! callback is ever passed into the kernel. Frees the SM with `kdf_sm_free` on scope exit — including
//! on any exception thrown by a member, so the driver loop needs no manual cleanup.
class ScanStateMachine {
public:
	ScanStateMachine() = default;
	~ScanStateMachine() { reset(); }

	ScanStateMachine(ScanStateMachine &&o) noexcept : sm_(o.sm_) { o.sm_ = nullptr; }
	ScanStateMachine &operator=(ScanStateMachine &&o) noexcept {
		if (this != &o) {
			reset();
			sm_ = o.sm_;
			o.sm_ = nullptr;
		}
		return *this;
	}
	ScanStateMachine(const ScanStateMachine &) = delete;
	ScanStateMachine &operator=(const ScanStateMachine &) = delete;

	//! Open a steppable scan SM over the Delta table at `path` (`version` < 0 = latest).
	//! `metadata_only` selects the file-list-terminal SM. `predicate`, if non-null, is a
	//! data-skipping `ffi::EnginePredicate` the kernel visits ONCE here (its borrowed engine state
	//! need only outlive this call). Throws `KernelException` on failure.
	static ScanStateMachine Open(const std::string &path, int64_t version, bool metadata_only,
	                             ffi::EnginePredicate *predicate = nullptr) {
		char *err = nullptr;
		ffi::KdfSM *sm =
		    ffi::kdf_scan_open(path.c_str(), path.size(), version, metadata_only, predicate, &err);
		if (!sm) {
			detail::ThrowKernelError("kdf_scan_open", err);
		}
		return ScanStateMachine(sm);
	}

	//! Advance the SM to the next Reduce or to Done. Throws on error.
	Step GetStep() {
		char *err = nullptr;
		int32_t kind = ffi::kdf_sm_get_step(sm_, &err);
		if (kind < 0) {
			detail::ThrowKernelError("kdf_sm_get_step", err);
		}
		return kind == ffi::KDF_STEP_DONE ? Step::Done : Step::Reduce;
	}

	//! The pending Reduce lowered to DuckDB SQL (legacy transport). Valid after GetStep()==Reduce.
	KernelString ReduceSql() {
		char *err = nullptr;
		char *sql = ffi::kdf_sm_reduce_sql(sm_, &err);
		if (!sql) {
			detail::ThrowKernelError("kdf_sm_reduce_sql", err);
		}
		return KernelString(sql);
	}

	//! The pending Reduce subplan as proto bytes (IR transport). Valid after GetStep()==Reduce.
	KernelBytes ReducePlan() {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::kdf_sm_reduce_plan(sm_, &len, &err);
		if (!buf) {
			detail::ThrowKernelError("kdf_sm_reduce_plan", err);
		}
		return KernelBytes(buf, len);
	}

	//! Hand the pending Reduce's result back as one Arrow C Data batch (ownership moves into the
	//! kernel; the structs are emptied). Throws on error.
	void SubmitReduce(ffi::FFI_ArrowArray *array, ffi::FFI_ArrowSchema *schema) {
		char *err = nullptr;
		if (ffi::kdf_sm_submit_reduce(sm_, array, schema, &err) != 0) {
			detail::ThrowKernelError("kdf_sm_submit_reduce", err);
		}
	}

	//! The terminal ResultPlan lowered to DuckDB SQL (legacy transport). Valid after GetStep()==Done.
	KernelString ResultSql() {
		char *err = nullptr;
		char *sql = ffi::kdf_sm_result_sql(sm_, &err);
		if (!sql) {
			detail::ThrowKernelError("kdf_sm_result_sql", err);
		}
		return KernelString(sql);
	}

	//! The terminal ResultPlan as proto bytes (IR transport). Valid after GetStep()==Done.
	KernelBytes ResultPlan() {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::kdf_sm_result_plan(sm_, &len, &err);
		if (!buf) {
			detail::ThrowKernelError("kdf_sm_result_plan", err);
		}
		return KernelBytes(buf, len);
	}

	//! Borrow the raw handle (e.g. for an ABI the SDK doesn't wrap yet). The SDK still owns it.
	ffi::KdfSM *get() const { return sm_; }
	bool valid() const { return sm_ != nullptr; }

private:
	explicit ScanStateMachine(ffi::KdfSM *sm) : sm_(sm) {}
	void reset() {
		if (sm_) {
			ffi::kdf_sm_free(sm_);
			sm_ = nullptr;
		}
	}
	ffi::KdfSM *sm_ = nullptr;
};

} // namespace sdk
} // namespace delta_kernel
