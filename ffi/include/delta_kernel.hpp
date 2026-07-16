//===----------------------------------------------------------------------===//
// delta_kernel.hpp — the kernel's ergonomic C++ interface over the raw C ABI.
//
// The generated `delta_kernel_ffi.hpp` exposes the plan-based scan as a flat C ABI (`delta_*` in
// namespace `ffi`): opaque handles, malloc'd error strings, proto bytes. This header layers the
// nice C++ API on top — the one the engine actually programs against:
//
//   auto sm   = delta::open_snapshot(path, version);   // StateMachine<Snapshot>
//   auto snap = delta::drive(std::move(sm), engine);    // Snapshot  (engine runs the reduces)
//   auto scan = snap.scan(engine, delta::ScanKind::Data, nullptr);  // Scan
//   const delta::plan::ResultPlan& p = scan.plan();     // parsed proto — faithfully executed
//
// Pure C++ out: values (`Snapshot`, `Scan`), parsed protos, `std::string` — no raw handles, no
// `_free`, no `ffi::` types in the public surface, no SQL. The word "reduce" never appears in the
// `Engine` interface: the engine is a plain IR-plan -> Arrow executor; the SDK feeds the kernel
// reducer behind its back.
//
// Errors are TWO directional types:
//   - DeltaError  (kernel -> engine): a kernel op failed. THROWN by next/build/getters.
//   - EngineError (engine -> kernel): the engine couldn't run a plan. A VALUE handed BACK via
//                 submit(EngineError) so the kernel can react. NOT thrown.
//
// This header returns PARSED protos, so it #includes the generated `.pb.h` and is therefore a
// C++17 header. An engine that pins C++11 for its other TUs must confine use of this header to a
// C++17 island (see the design docs). The FFI-header include is overridable via
// DELTA_KERNEL_FFI_HEADER for engines that consume a locally-patched copy.
//===----------------------------------------------------------------------===//
#pragma once

// The generated C ABI header (namespace `ffi`). Defaults to the kernel-shipped name; an engine that
// post-processes cbindgen output (e.g. DuckDB -> generated_delta_kernel_ffi.hpp) defines this first.
#ifndef DELTA_KERNEL_FFI_HEADER
#define DELTA_KERNEL_FFI_HEADER "delta_kernel_ffi.hpp"
#endif
#include DELTA_KERNEL_FFI_HEADER

#include "plan.pb.h"    // delta::kernel::plan::{Plan, ResultPlan}
#include "schema.pb.h"  // delta::kernel::schema::StructType

#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace delta {

// Re-export the parsed proto namespaces so callers write `delta::plan::...`, `delta::schema::...`.
namespace plan = ::delta::kernel::plan;
namespace schema = ::delta::kernel::schema;

//===----------------------------------------------------------------------===//
// Errors — two directional types
//===----------------------------------------------------------------------===//

/// kernel -> engine: a kernel operation failed (bad log / schema / invariant). Thrown by
/// `next`/`build`/value getters. Carries the kernel's message.
class DeltaError : public std::runtime_error {
public:
	explicit DeltaError(const std::string &msg) : std::runtime_error(msg) {}
};

/// engine -> kernel: the engine could not execute a plan. A VALUE the engine produces and hands
/// back via `StateMachine::submit(EngineError)` so the kernel can react. Not thrown by the SDK.
struct EngineError {
	std::string message;
};

namespace detail {

/// Turn a `delta_*` `out_err` slot into a thrown `DeltaError` (frees the kernel string). `what`
/// names the failing op. Never returns.
[[noreturn]] inline void Throw(const char *what, char *err) {
	std::string msg = err ? std::string(err) : std::string("unknown error");
	if (err) {
		ffi::delta_string_free(err);
	}
	throw DeltaError(std::string(what) + " failed: " + msg);
}

/// Decode a `(uint8_t*, len)` proto buffer from a `delta_*_plan`/`_schema` emitter into a parsed
/// proto message `M`, freeing the kernel buffer. Throws `DeltaError` on a null/undecodable buffer.
/// `err` is the emitter's already-populated out_err (null on success).
template <class M>
inline M DecodeBytes(const char *what, uint8_t *buf, size_t len, char *err) {
	if (!buf) {
		detail::Throw(what, err);
	}
	M msg;
	bool ok = msg.ParseFromArray(buf, static_cast<int>(len));
	ffi::delta_bytes_free(buf, len);
	if (!ok) {
		throw DeltaError(std::string(what) + ": proto bytes did not decode");
	}
	return msg;
}

} // namespace detail

//===----------------------------------------------------------------------===//
// Arrow + the reducer
//===----------------------------------------------------------------------===//

enum class ScanKind { Metadata, Data };

/// One Arrow C Data batch (schema + array). Uses the FFI's Arrow C-Data structs (`ffi::FFI_Arrow*`,
/// ABI-identical to the Arrow C Data `ArrowArray`/`ArrowSchema`). Moving one into the kernel (via
/// `Reducer::apply`) empties the source structs (the Arrow C Data ownership convention).
struct ArrowBatch {
	ffi::FFI_ArrowArray array;
	ffi::FFI_ArrowSchema schema;
};

/// A lazy stream of Arrow batches: each call yields the next batch, or `nullopt` at end. How an
/// `Engine` returns a plan's output — e.g. a lambda over DuckDB's `Fetch()` loop.
using ArrowStream = std::function<std::optional<ArrowBatch>()>;

/// Whether the reducer wants more input.
enum class Control { Continue, Break };

/// The finished reducer, produced by `Reducer::finish`/`apply_all`; handed back via
/// `StateMachine::submit`. Opaque; owns the kernel `DeltaFinishedReducer` handle.
class FinishedReducer {
public:
	FinishedReducer() = default;
	explicit FinishedReducer(ffi::DeltaFinishedReducer *h) : h_(h) {}
	FinishedReducer(FinishedReducer &&o) noexcept : h_(o.h_) { o.h_ = nullptr; }
	FinishedReducer &operator=(FinishedReducer &&o) noexcept {
		if (this != &o) {
			h_ = o.h_;
			o.h_ = nullptr;
		}
		return *this;
	}
	FinishedReducer(const FinishedReducer &) = delete;
	FinishedReducer &operator=(const FinishedReducer &) = delete;

	//! Release the handle to the caller (consumed by `StateMachine::submit`).
	ffi::DeltaFinishedReducer *release() {
		auto *h = h_;
		h_ = nullptr;
		return h;
	}

private:
	// A finished reducer is a terminal token with no dedicated free — it is always consumed by
	// submit(). If dropped unconsumed the handle simply leaks (never happens on the normal path).
	ffi::DeltaFinishedReducer *h_ = nullptr;
};

/// The kernel reducer for a pending Reduce, moved out of the state machine. Feed it the engine's
/// Arrow output batch-by-batch (`apply`) then `finish` (or `apply_all` for the common case). Owns the
/// kernel `DeltaReducer` handle; freed on scope exit if never finished.
class Reducer {
public:
	Reducer() = default;
	explicit Reducer(ffi::DeltaReducer *h) : h_(h) {}
	~Reducer() { reset(); }
	Reducer(Reducer &&o) noexcept : h_(o.h_) { o.h_ = nullptr; }
	Reducer &operator=(Reducer &&o) noexcept {
		if (this != &o) {
			reset();
			h_ = o.h_;
			o.h_ = nullptr;
		}
		return *this;
	}
	Reducer(const Reducer &) = delete;
	Reducer &operator=(const Reducer &) = delete;

	//! Feed one Arrow batch (ownership moves into the kernel; the structs are emptied). Returns
	//! whether the reducer wants more input. Throws `DeltaError` on error.
	Control apply(ArrowBatch &batch) {
		char *err = nullptr;
		int32_t r = ffi::delta_reducer_apply(h_, &batch.array, &batch.schema, &err);
		if (r < 0) {
			detail::Throw("delta_reducer_apply", err);
		}
		return r == 1 ? Control::Break : Control::Continue;
	}

	//! Finish the reducer, consuming it → the finished token for `submit`. Throws on error.
	FinishedReducer finish() && {
		char *err = nullptr;
		ffi::DeltaFinishedReducer *f = ffi::delta_reducer_finish(h_, &err);
		h_ = nullptr; // finish consumes the kernel handle
		if (!f) {
			detail::Throw("delta_reducer_finish", err);
		}
		return FinishedReducer(f);
	}

	//! Convenience: pull the whole stream, `apply` each (stopping on `Break`), then `finish`.
	FinishedReducer apply_all(ArrowStream stream) && {
		while (auto batch = stream()) {
			if (apply(*batch) == Control::Break) {
				break;
			}
		}
		return std::move(*this).finish();
	}

private:
	void reset() {
		if (h_) {
			ffi::delta_reducer_free(h_);
			h_ = nullptr;
		}
	}
	ffi::DeltaReducer *h_ = nullptr;
};

//===----------------------------------------------------------------------===//
// Requests & results (sum types)
//===----------------------------------------------------------------------===//

/// The kernel needs the engine to run this reduce plan, then hand back the reduced result. Carries
/// the parsed plan and the reducer (moved out of the SM).
struct Reduce {
	plan::Plan plan;
	Reducer reducer;
};
/// The state machine is finished; call `build()`.
struct Done {};

/// What `StateMachine::next()` returned.
using EngineRequest = std::variant<Reduce, Done>;

/// The result of satisfying a `Reduce` request: the finished reducer. A variant to mirror
/// `EngineRequest` and leave room for future request kinds.
struct ReduceResult {
	FinishedReducer reducer;
};
using EngineResult = std::variant<ReduceResult>;

//===----------------------------------------------------------------------===//
// The engine (you implement this) — a pure IR-plan -> Arrow executor
//===----------------------------------------------------------------------===//

class Engine {
public:
	virtual ~Engine() = default;
	//! Execute an IR plan and yield its output as a lazy Arrow batch stream. Knows nothing of
	//! reducers or state machines.
	virtual ArrowStream execute_to_arrow(const plan::Plan &) = 0;
};

//===----------------------------------------------------------------------===//
// Values — built snapshot & scan (inert; parsed protos out)
//===----------------------------------------------------------------------===//

class Scan;    // forward
template <class T>
class StateMachine; // forward

/// A built, point-in-time snapshot. Answers version/schema and builds scans. Owns the kernel
/// `DeltaSnapshotValue` handle.
class Snapshot {
public:
	Snapshot() = default;
	explicit Snapshot(ffi::DeltaSnapshotValue *h) : h_(h) {}
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

	int64_t version() const { return ffi::delta_snapshot_version(h_); }

	//! The snapshot's logical schema (parsed proto).
	schema::StructType schema() const {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::delta_snapshot_schema(h_, &len, &err);
		return detail::DecodeBytes<schema::StructType>("delta_snapshot_schema", buf, len, err);
	}

	//! Build a scan state machine off this snapshot (async: you drive it). `predicate`, if non-null,
	//! is a data-skipping `ffi::EnginePredicate` visited once here.
	StateMachine<Scan> scan_sm(ScanKind kind, ffi::EnginePredicate *predicate = nullptr) const;

	//! Build a scan value directly, driving the scan SM to completion via `engine` (sync).
	Scan scan(Engine &engine, ScanKind kind, ffi::EnginePredicate *predicate = nullptr) const;

	ffi::DeltaSnapshotValue *get() const { return h_; }

private:
	void reset() {
		if (h_) {
			ffi::delta_snapshot_free(h_);
			h_ = nullptr;
		}
	}
	ffi::DeltaSnapshotValue *h_ = nullptr;
};

/// A built scan. Emits the plan the engine faithfully executes (parsed proto). Owns the kernel
/// `DeltaScanValue` handle.
class Scan {
public:
	Scan() = default;
	explicit Scan(ffi::DeltaScanValue *h) : h_(h) {}
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

	//! The scan's terminal plan (parsed proto) for its `ScanKind`.
	plan::ResultPlan plan() const {
		char *err = nullptr;
		size_t len = 0;
		uint8_t *buf = ffi::delta_scan_plan(h_, &len, &err);
		return detail::DecodeBytes<plan::ResultPlan>("delta_scan_plan", buf, len, err);
	}

	ffi::DeltaScanValue *get() const { return h_; }

private:
	void reset() {
		if (h_) {
			ffi::delta_scan_free(h_);
			h_ = nullptr;
		}
	}
	ffi::DeltaScanValue *h_ = nullptr;
};

//===----------------------------------------------------------------------===//
// The state machine — one generic; T ∈ {Snapshot, Scan}
//===----------------------------------------------------------------------===//

namespace detail {
constexpr int32_t kStepDone = 0;   // ffi::DELTA_STEP_DONE
constexpr int32_t kStepReduce = 1; // ffi::DELTA_STEP_REDUCE
} // namespace detail

/// Move-only owner of the kernel `DeltaSm` handle. Drivable: `next` → a `Reduce`/`Done` request;
/// `submit` hands back the reduce's result (or an engine failure); `build` consumes the SM → the
/// value `T`. The kernel is passive — you own the loop; no callback is passed into the kernel.
template <class T>
class StateMachine {
public:
	StateMachine() = default;
	explicit StateMachine(ffi::DeltaSm *h) : h_(h) {}
	~StateMachine() { reset(); }
	StateMachine(StateMachine &&o) noexcept : h_(o.h_) { o.h_ = nullptr; }
	StateMachine &operator=(StateMachine &&o) noexcept {
		if (this != &o) {
			reset();
			h_ = o.h_;
			o.h_ = nullptr;
		}
		return *this;
	}
	StateMachine(const StateMachine &) = delete;
	StateMachine &operator=(const StateMachine &) = delete;

	//! Advance to the next Reduce or to Done. On Reduce, assembles the parsed plan + the reducer
	//! (moved out of the SM). Throws `DeltaError` on kernel failure.
	EngineRequest next() {
		char *err = nullptr;
		int32_t step = ffi::delta_sm_next(h_, &err);
		if (step < 0) {
			detail::Throw("delta_sm_next", err);
		}
		if (step == detail::kStepDone) {
			return Done{};
		}
		// Reduce: pull the plan bytes + the reducer.
		size_t len = 0;
		uint8_t *buf = ffi::delta_sm_reduce_plan(h_, &len, &err);
		plan::Plan p = detail::DecodeBytes<plan::Plan>("delta_sm_reduce_plan", buf, len, err);
		char *rerr = nullptr;
		ffi::DeltaReducer *rd = ffi::delta_sm_take_reducer(h_, &rerr);
		if (!rd) {
			detail::Throw("delta_sm_take_reducer", rerr);
		}
		return Reduce{std::move(p), Reducer(rd)};
	}

	//! Hand the reduce's result back to the SM, advancing it. Throws on error.
	void submit(EngineResult result) {
		auto &rr = std::get<ReduceResult>(result);
		char *err = nullptr;
		if (ffi::delta_sm_submit(h_, rr.reducer.release(), &err) != 0) {
			detail::Throw("delta_sm_submit", err);
		}
	}

	//! Report that the engine FAILED to execute the pending reduce (engine -> kernel direction). The
	//! SM is poisoned; this surfaces the failure as a thrown `DeltaError` (the failure is real).
	void submit(const EngineError &e) {
		char *err = nullptr;
		ffi::delta_sm_submit_error(h_, e.message.c_str(), e.message.size(), &err);
		detail::Throw("engine reduce", err);
	}

	//! Consume the SM → the built value `T`. Valid only after `next()` returned `Done`. Throws on a
	//! wrong-arm SM or an unfinished drive.
	T build() &&;

	ffi::DeltaSm *get() const { return h_; }

private:
	void reset() {
		if (h_) {
			ffi::delta_sm_free(h_);
			h_ = nullptr;
		}
	}
	ffi::DeltaSm *h_ = nullptr;
};

// build() specializations — call the terminal-specific export. The C++ template keeps T, so no
// result-union is needed; a wrong-arm SM is rejected by the kernel export.
template <>
inline Snapshot StateMachine<Snapshot>::build() && {
	char *err = nullptr;
	ffi::DeltaSnapshotValue *v = ffi::delta_sm_build_snapshot(h_, &err);
	h_ = nullptr; // build consumes the SM
	if (!v) {
		detail::Throw("delta_sm_build_snapshot", err);
	}
	return Snapshot(v);
}
template <>
inline Scan StateMachine<Scan>::build() && {
	char *err = nullptr;
	ffi::DeltaScanValue *v = ffi::delta_sm_build_scan(h_, &err);
	h_ = nullptr;
	if (!v) {
		detail::Throw("delta_sm_build_scan", err);
	}
	return Scan(v);
}

//===----------------------------------------------------------------------===//
// Entry point + the sync driver
//===----------------------------------------------------------------------===//

/// Open a snapshot state machine over the Delta table at `path` (`version` < 0 = latest). Throws
/// `DeltaError` on failure.
inline StateMachine<Snapshot> open_snapshot(const std::string &path, int64_t version = -1) {
	char *err = nullptr;
	ffi::DeltaSm *sm = ffi::delta_open_snapshot(path.c_str(), path.size(), version, &err);
	if (!sm) {
		detail::Throw("delta_open_snapshot", err);
	}
	return StateMachine<Snapshot>(sm);
}

/// Drive a state machine to its value via an `Engine`, executing each Reduce (blocks). A kernel
/// failure (`DeltaError` from `next`/`build`) propagates; an engine failure is handed back to the
/// kernel via `submit`, never swallowed.
template <class T>
inline T drive(StateMachine<T> sm, Engine &engine) {
	for (;;) {
		EngineRequest req = sm.next();
		if (std::holds_alternative<Done>(req)) {
			return std::move(sm).build();
		}
		auto &r = std::get<Reduce>(req);
		sm.submit(EngineResult{ReduceResult{std::move(r.reducer).apply_all(engine.execute_to_arrow(r.plan))}});
	}
}

//===----------------------------------------------------------------------===//
// Snapshot scan factories (defined after StateMachine/Scan/drive are complete)
//===----------------------------------------------------------------------===//

inline StateMachine<Scan> Snapshot::scan_sm(ScanKind kind, ffi::EnginePredicate *predicate) const {
	char *err = nullptr;
	int32_t k = (kind == ScanKind::Metadata) ? 0 : 1;
	ffi::DeltaSm *sm = ffi::delta_snapshot_scan_sm(h_, k, predicate, &err);
	if (!sm) {
		detail::Throw("delta_snapshot_scan_sm", err);
	}
	return StateMachine<Scan>(sm);
}

inline Scan Snapshot::scan(Engine &engine, ScanKind kind, ffi::EnginePredicate *predicate) const {
	return drive(scan_sm(kind, predicate), engine);
}

} // namespace delta
