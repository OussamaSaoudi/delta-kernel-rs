//! DataFusion execution for Delta Kernel declarative plans.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::execution::context::SessionContext;
use datafusion::object_store::ObjectStore as DataFusionObjectStore;
use datafusion_execution::config::SessionConfig;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use delta_kernel::object_store::local::LocalFileSystem;
use delta_kernel::plans::ir::operation::{IoOperation, Operation};
use delta_kernel::plans::ir::plan::Plan;
use delta_kernel::plans::{PlanExecutor, PlanResult};
use delta_kernel::scan::Scan;
use delta_kernel::{
    DeltaResult, Engine, Error, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler,
};
use delta_kernel_default_engine::DefaultEngineBuilder;
use url::Url;

use crate::compile::{compile_plan, CompileContext};
use crate::error::DfResultIntoDelta;

fn default_kernel_engine() -> Arc<dyn Engine> {
    Arc::new(DefaultEngineBuilder::new(Arc::new(LocalFileSystem::new())).build())
}

/// Compiler and executor for Delta Kernel plans using a DataFusion session.
#[derive(Clone)]
pub struct DataFusionExecutor {
    session_ctx: SessionContext,
    engine: Arc<dyn Engine>,
}

impl DataFusionExecutor {
    /// Creates an executor backed by a local-filesystem default kernel engine.
    pub fn try_new() -> DeltaResult<Self> {
        Self::try_new_with_engine(default_kernel_engine())
    }

    /// Creates an executor using `engine` for storage and deletion-vector reads.
    pub fn try_new_with_engine(engine: Arc<dyn Engine>) -> DeltaResult<Self> {
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .optimizer
            .enable_leaf_expression_pushdown = false;
        Ok(Self {
            session_ctx: SessionContext::new_with_config(config),
            engine,
        })
    }

    /// Returns the fallback kernel engine used for I/O helpers.
    pub fn engine(&self) -> &Arc<dyn Engine> {
        &self.engine
    }

    /// Returns the DataFusion session used to optimize and execute plans.
    pub fn session_context(&self) -> &SessionContext {
        &self.session_ctx
    }

    /// Registers an object store for DataFusion scans under `url`.
    pub fn register_object_store(
        &self,
        url: &Url,
        store: Arc<dyn DataFusionObjectStore>,
    ) -> Option<Arc<dyn DataFusionObjectStore>> {
        self.session_ctx.register_object_store(url, store)
    }

    /// Compiles a kernel plan into an interactive DataFusion dataframe.
    pub fn plan_to_dataframe(&self, plan: &Plan) -> DeltaResult<DataFrame> {
        let logical =
            compile_plan(plan, &CompileContext::new(Arc::clone(&self.engine))).into_delta()?;
        Ok(DataFrame::new(self.session_ctx.state(), logical))
    }

    /// Builds and compiles the latest declarative metadata scan plan.
    pub fn scan_metadata(&self, scan: &Scan) -> DeltaResult<Option<DataFrame>> {
        let engine = ExecutorEngine {
            fallback: Arc::clone(&self.engine),
            executor: Arc::new(self.clone()),
        };
        scan.declarative_metadata_scan_plan(&engine)?
            .as_ref()
            .map(|plan| self.plan_to_dataframe(plan))
            .transpose()
    }

    fn execute_query(&self, plan: Plan) -> DeltaResult<PlanResult> {
        let executor = self.clone();
        let join = std::thread::Builder::new()
            .name("delta-datafusion-plan".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| Error::generic(e.to_string()))?;
                runtime.block_on(async move {
                    executor
                        .plan_to_dataframe(&plan)?
                        .collect()
                        .await
                        .into_delta()
                })
            })
            .map_err(|e| Error::generic(e.to_string()))?;
        let batches = join
            .join()
            .map_err(|_| Error::generic("DataFusion plan thread panicked"))??;
        Ok(PlanResult::Data(Box::new(batches.into_iter().map(
            |batch| Ok(Box::new(ArrowEngineData::new(batch)) as Box<dyn delta_kernel::EngineData>),
        ))))
    }
}

impl PlanExecutor for DataFusionExecutor {
    fn execute_op(&self, op: Operation) -> DeltaResult<PlanResult> {
        match op {
            Operation::QueryPlan(plan) => self.execute_query(plan),
            Operation::IoOperation(IoOperation::ParquetFooter { file }) => self
                .engine
                .parquet_handler()
                .read_parquet_footer(&file)
                .map(PlanResult::ParquetFooter),
            Operation::IoOperation(other) => Err(Error::unsupported(format!(
                "DataFusion executor does not implement I/O operation {other:?}"
            ))),
        }
    }
}

struct ExecutorEngine {
    fallback: Arc<dyn Engine>,
    executor: Arc<dyn PlanExecutor>,
}

impl Engine for ExecutorEngine {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.fallback.evaluation_handler()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.fallback.storage_handler()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.fallback.json_handler()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.fallback.parquet_handler()
    }

    fn plan_executor(&self) -> Option<Arc<dyn PlanExecutor>> {
        Some(Arc::clone(&self.executor))
    }
}
