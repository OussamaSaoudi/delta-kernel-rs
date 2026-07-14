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
// The state machines
//
// The read path is two steppable SMs driven in sequence, mirroring the kernel's domain model:
//   Snapshot::Open(path, version) -> drive to Done -> Snapshot::Scan(...) -> Scan -> drive to Done.
// Both are driven identically (GetStep/ReduceSql/ReducePlan/SubmitReduce until Done); they differ in
// their terminal — a Snapshot yields a built snapshot (queryable version, scan factory), a Scan
// yields the ResultPlan the engine executes.
//===----------------------------------------------------------------------===//

//! What a `GetStep()` returned: the kernel needs a Reduce run, or the SM is done.
enum class Step {
	Reduce, //! KDF_STEP_REDUCE: fetch the pending reduce (SQL or plan), run it, SubmitReduce the Arrow.
	Done,   //! KDF_STEP_DONE: the SM finished; fetch its terminal.
};

class Scan; // defined below; Snapshot::Scan() returns one.

//! Move-only RAII owner of a `KdfSnapshot*`. Opens the snapshot SM (`Open`), is driven to a built
//! point-in-time snapshot (`GetStep`/`Reduce*`/`SubmitReduce` until Done), then reports its `Version`
//! and builds `Scan`s (`Scan(...)`). Frees with `kdf_snapshot_free` on scope exit — including on any
//! thrown exception, so the driver loop needs no manual cleanup. No engine callback passes into the
//! kernel; the engine drives.
class Snapshot {
public:
	Snapshot() = default;
	~Snapshot() { reset(); }

	Snapshot(Snapshot &&o) noexcept : h_(o.h_) { o.h_ = nullptr; }
	Snapshot &operator=(Snapshot &&o) noexcept {
		if (this != &o) {
			reset();
			h_ = o.h_;
			o.h_ = nullptr;
		}
		return *this;
	}
	Snapshot(const Snapshot &) = delete;
	Snapshot &operator=(const Snapshot &) = delete;

	//! Open a steppable snapshot SM over the Delta table at `path` (`version` < 0 = latest).
	//! Throws `KernelException` on failure.
	static Snapshot Open(const std::string &path, int64_t version) {
		char *err = nullptr;
		ffi::KdfSnapshot *h = ffi::kdf_snapshot_open(path.c_str(), path.size(), version, &err);
		if (!h) {
			detail::ThrowKernelError("kdf_snapshot_open", err);
		}
		return Snapshot(h);
	}

	//! Advance the snapshot SM to the next Reduce or to Done. Throws on error.
	Step GetStep() {
		char *err = nullptr;
		int32_t kind = ffi::kdf_snapshot_get_step(h_, &err);
		if (kind < 0) {
			detail::ThrowKernelError("kdf_snapshot_get_step", err);
		}
		return kind == ffi::KDF_STEP_DONE ? Step::Done : Step::Reduce;
	}

	//! The pending Reduce lowered to DuckDB SQL. Valid after GetStep()==Reduce.
	KernelString ReduceSql() {
		char *err = nullptr;
		char *sql = ffi::kdf_snapshot_reduce_sql(h_, &err);
		if (!sql) {
			detail::ThrowKernelError("kdf_snapshot_reduce_sql", err);
		}
		return KernelString(sql);
	}

	//! The pending Reduce subplan as proto bytes (IR transport). Valid after GetStep()==Reduce.
	KernelBytes ReducePlan() {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::kdf_snapshot_reduce_plan(h_, &len, &err);
		if (!buf) {
			detail::ThrowKernelError("kdf_snapshot_reduce_plan", err);
		}
		return KernelBytes(buf, len);
	}

	//! Hand the pending Reduce's result back as one Arrow C Data batch (ownership moves in; the
	//! structs are emptied). Throws on error.
	void SubmitReduce(ffi::FFI_ArrowArray *array, ffi::FFI_ArrowSchema *schema) {
		char *err = nullptr;
		if (ffi::kdf_snapshot_submit_reduce(h_, array, schema, &err) != 0) {
			detail::ThrowKernelError("kdf_snapshot_submit_reduce", err);
		}
	}

	//! The finished snapshot's version. Valid after the SM reached Done. Throws on error.
	int64_t Version() {
		char *err = nullptr;
		int64_t v = ffi::kdf_snapshot_version(h_, &err);
		if (v < 0) {
			detail::ThrowKernelError("kdf_snapshot_version", err);
		}
		return v;
	}

	//! Build a scan off the finished snapshot. `metadata_only` selects the file-list-terminal SM;
	//! `predicate`, if non-null, is a data-skipping `ffi::EnginePredicate` the kernel visits ONCE here
	//! (its borrowed engine state need only outlive this call). The snapshot is unchanged and can
	//! build further scans. Throws on error. (Defined out-of-line below, after `Scan`.)
	class Scan Scan(bool metadata_only, ffi::EnginePredicate *predicate = nullptr);

	ffi::KdfSnapshot *get() const { return h_; }
	bool valid() const { return h_ != nullptr; }

private:
	explicit Snapshot(ffi::KdfSnapshot *h) : h_(h) {}
	void reset() {
		if (h_) {
			ffi::kdf_snapshot_free(h_);
			h_ = nullptr;
		}
	}
	ffi::KdfSnapshot *h_ = nullptr;
};

//! Move-only RAII owner of a `KdfScan*` (built from a `Snapshot`). Driven to a terminal ResultPlan
//! (`GetStep`/`Reduce*`/`SubmitReduce` until Done), then yields it as SQL (`ResultSql`) or proto
//! bytes (`ResultPlan`). Frees with `kdf_scan_free` on scope exit, incl. on throw.
class Scan {
public:
	Scan() = default;
	~Scan() { reset(); }

	Scan(Scan &&o) noexcept : h_(o.h_) { o.h_ = nullptr; }
	Scan &operator=(Scan &&o) noexcept {
		if (this != &o) {
			reset();
			h_ = o.h_;
			o.h_ = nullptr;
		}
		return *this;
	}
	Scan(const Scan &) = delete;
	Scan &operator=(const Scan &) = delete;

	//! Wrap a raw `KdfScan*` (from `kdf_snapshot_scan`); takes ownership. Prefer `Snapshot::Scan`.
	explicit Scan(ffi::KdfScan *h) : h_(h) {}

	//! Advance the scan SM to the next Reduce or to Done. Throws on error.
	Step GetStep() {
		char *err = nullptr;
		int32_t kind = ffi::kdf_scan_get_step(h_, &err);
		if (kind < 0) {
			detail::ThrowKernelError("kdf_scan_get_step", err);
		}
		return kind == ffi::KDF_STEP_DONE ? Step::Done : Step::Reduce;
	}

	//! The pending Reduce lowered to DuckDB SQL. Valid after GetStep()==Reduce.
	KernelString ReduceSql() {
		char *err = nullptr;
		char *sql = ffi::kdf_scan_reduce_sql(h_, &err);
		if (!sql) {
			detail::ThrowKernelError("kdf_scan_reduce_sql", err);
		}
		return KernelString(sql);
	}

	//! The pending Reduce subplan as proto bytes (IR transport). Valid after GetStep()==Reduce.
	KernelBytes ReducePlan() {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::kdf_scan_reduce_plan(h_, &len, &err);
		if (!buf) {
			detail::ThrowKernelError("kdf_scan_reduce_plan", err);
		}
		return KernelBytes(buf, len);
	}

	//! Hand the pending Reduce's result back as one Arrow C Data batch (ownership moves in; the
	//! structs are emptied). Throws on error.
	void SubmitReduce(ffi::FFI_ArrowArray *array, ffi::FFI_ArrowSchema *schema) {
		char *err = nullptr;
		if (ffi::kdf_scan_submit_reduce(h_, array, schema, &err) != 0) {
			detail::ThrowKernelError("kdf_scan_submit_reduce", err);
		}
	}

	//! The terminal ResultPlan lowered to DuckDB SQL. Valid after GetStep()==Done.
	KernelString ResultSql() {
		char *err = nullptr;
		char *sql = ffi::kdf_scan_result_sql(h_, &err);
		if (!sql) {
			detail::ThrowKernelError("kdf_scan_result_sql", err);
		}
		return KernelString(sql);
	}

	//! The terminal ResultPlan as proto bytes (IR transport). Valid after GetStep()==Done.
	KernelBytes ResultPlan() {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::kdf_scan_result_plan(h_, &len, &err);
		if (!buf) {
			detail::ThrowKernelError("kdf_scan_result_plan", err);
		}
		return KernelBytes(buf, len);
	}

	ffi::KdfScan *get() const { return h_; }
	bool valid() const { return h_ != nullptr; }

private:
	void reset() {
		if (h_) {
			ffi::kdf_scan_free(h_);
			h_ = nullptr;
		}
	}
	ffi::KdfScan *h_ = nullptr;
};

inline Scan Snapshot::Scan(bool metadata_only, ffi::EnginePredicate *predicate) {
	char *err = nullptr;
	ffi::KdfScan *h = ffi::kdf_snapshot_scan(h_, metadata_only, predicate, &err);
	if (!h) {
		detail::ThrowKernelError("kdf_snapshot_scan", err);
	}
	return sdk::Scan(h);
}

} // namespace sdk
} // namespace delta_kernel
