//! Expression lowering: Kernel Expression/Predicate -> DataFusion Expr.

use datafusion_expr::Expr;
use delta_kernel::expressions::{Expression, Predicate};

use crate::error::{DfResult, DfError};

/// Lower a kernel Expression to a DataFusion Expr.
pub fn lower_expression(expr: &Expression) -> DfResult<Expr> {
    Err(DfError::Unsupported("Expression lowering not yet implemented".to_string()))
}

/// Lower a kernel Predicate to a DataFusion boolean Expr.
pub fn lower_predicate(pred: &Predicate) -> DfResult<Expr> {
    Err(DfError::Unsupported("Predicate lowering not yet implemented".to_string()))
}

