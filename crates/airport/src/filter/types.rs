use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// ExpressionClass identifies the category of expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpressionClass {
    BoundAggregate,
    BoundCase,
    BoundCast,
    BoundColumnRef,
    BoundComparison,
    BoundConjunction,
    BoundConstant,
    BoundDefault,
    BoundFunction,
    BoundOperator,
    BoundParameter,
    BoundRef,
    BoundSubquery,
    BoundWindow,
    BoundBetween,
    BoundUnnest,
    BoundLambda,
    BoundLambdaRef,
    #[serde(other)]
    Unknown,
}

/// ExpressionType identifies the specific operation type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpressionType {
    // Comparison operators
    CompareEqual,
    CompareNotequal,
    CompareLessthan,
    CompareGreaterthan,
    CompareLessthanorequalto,
    CompareGreaterthanorequalto,
    CompareIn,
    CompareNotIn,
    CompareDistinctFrom,
    CompareBetween,
    CompareNotBetween,
    CompareNotDistinctFrom,
    // Conjunction
    ConjunctionAnd,
    ConjunctionOr,
    // Unary operators
    OperatorNot,
    OperatorIsNull,
    OperatorIsNotNull,
    OperatorNullif,
    OperatorCoalesce,
    // Value types
    ValueConstant,
    ValueParameter,
    ValueNull,
    ValueDefault,
    // Function types
    Function,
    BoundFunction,
    // Aggregate
    Aggregate,
    BoundAggregate,
    // Window
    WindowAggregate,
    WindowRank,
    WindowRankDense,
    WindowNtile,
    WindowPercentRank,
    WindowCumeDist,
    WindowRowNumber,
    WindowFirstValue,
    WindowLastValue,
    WindowLead,
    WindowLag,
    WindowNthValue,
    // Other
    CaseExpr,
    ArrayExtract,
    ArraySlice,
    StructExtract,
    ArrayConstructor,
    Cast,
    BoundRef,
    BoundColumnRef,
    BoundUnnest,
    Lambda,
    #[serde(other)]
    Unknown,
}

/// ColumnBinding identifies a column by table and column index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnBinding {
    pub table_index: i32,
    pub column_index: i32,
}

/// LogicalType represents a DuckDB logical type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogicalType {
    pub id: String,
    #[serde(default)]
    pub type_info: Option<JsonValue>,
}

/// Value represents a literal constant value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstantValue {
    #[serde(rename = "type")]
    pub value_type: LogicalType,
    pub value: JsonValue,
    #[serde(default)]
    pub is_null: bool,
}

/// FilterPushdown is the top-level container for parsed filter JSON.
#[derive(Debug, Clone, Default)]
pub struct FilterPushdown {
    /// Parsed filter expressions. Multiple filters are implicitly AND'ed.
    pub filters: Vec<Expression>,
    /// Maps column binding indices to column names.
    pub column_bindings: Vec<String>,
}

impl FilterPushdown {
    /// Resolves a column name from a ColumnRefExpression binding.
    pub fn column_name(&self, binding: &ColumnBinding) -> Option<&str> {
        let idx = binding.column_index as usize;
        self.column_bindings.get(idx).map(String::as_str)
    }
}

/// Expression is an enum of all filter expression types.
#[derive(Debug, Clone)]
pub enum Expression {
    Comparison(ComparisonExpression),
    Conjunction(ConjunctionExpression),
    Constant(ConstantExpression),
    ColumnRef(ColumnRefExpression),
    Function(FunctionExpression),
    Cast(CastExpression),
    Between(BetweenExpression),
    Operator(OperatorExpression),
    Case(CaseExpression),
    Parameter(ParameterExpression),
    Reference(ReferenceExpression),
    Aggregate(AggregateExpression),
    Window(WindowExpression),
    Unsupported(UnsupportedExpression),
}

impl Expression {
    pub fn expr_class(&self) -> &ExpressionClass {
        match self {
            Expression::Comparison(e) => &e.base.expression_class,
            Expression::Conjunction(e) => &e.base.expression_class,
            Expression::Constant(e) => &e.base.expression_class,
            Expression::ColumnRef(e) => &e.base.expression_class,
            Expression::Function(e) => &e.base.expression_class,
            Expression::Cast(e) => &e.base.expression_class,
            Expression::Between(e) => &e.base.expression_class,
            Expression::Operator(e) => &e.base.expression_class,
            Expression::Case(e) => &e.base.expression_class,
            Expression::Parameter(e) => &e.base.expression_class,
            Expression::Reference(e) => &e.base.expression_class,
            Expression::Aggregate(e) => &e.base.expression_class,
            Expression::Window(e) => &e.base.expression_class,
            Expression::Unsupported(e) => &e.base.expression_class,
        }
    }

    pub fn expr_type(&self) -> &ExpressionType {
        match self {
            Expression::Comparison(e) => &e.base.expr_type,
            Expression::Conjunction(e) => &e.base.expr_type,
            Expression::Constant(e) => &e.base.expr_type,
            Expression::ColumnRef(e) => &e.base.expr_type,
            Expression::Function(e) => &e.base.expr_type,
            Expression::Cast(e) => &e.base.expr_type,
            Expression::Between(e) => &e.base.expr_type,
            Expression::Operator(e) => &e.base.expr_type,
            Expression::Case(e) => &e.base.expr_type,
            Expression::Parameter(e) => &e.base.expr_type,
            Expression::Reference(e) => &e.base.expr_type,
            Expression::Aggregate(e) => &e.base.expr_type,
            Expression::Window(e) => &e.base.expr_type,
            Expression::Unsupported(e) => &e.base.expr_type,
        }
    }

    pub fn alias(&self) -> &str {
        match self {
            Expression::Comparison(e) => &e.base.alias,
            Expression::Conjunction(e) => &e.base.alias,
            Expression::Constant(e) => &e.base.alias,
            Expression::ColumnRef(e) => &e.base.alias,
            Expression::Function(e) => &e.base.alias,
            Expression::Cast(e) => &e.base.alias,
            Expression::Between(e) => &e.base.alias,
            Expression::Operator(e) => &e.base.alias,
            Expression::Case(e) => &e.base.alias,
            Expression::Parameter(e) => &e.base.alias,
            Expression::Reference(e) => &e.base.alias,
            Expression::Aggregate(e) => &e.base.alias,
            Expression::Window(e) => &e.base.alias,
            Expression::Unsupported(e) => &e.base.alias,
        }
    }
}

/// BaseExpression contains common fields for all expression types.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BaseExpression {
    pub expression_class: ExpressionClass,
    #[serde(rename = "type")]
    pub expr_type: ExpressionType,
    #[serde(default)]
    pub alias: String,
}

impl Default for ExpressionClass {
    fn default() -> Self {
        ExpressionClass::Unknown
    }
}

impl Default for ExpressionType {
    fn default() -> Self {
        ExpressionType::Unknown
    }
}

#[derive(Debug, Clone)]
pub struct ComparisonExpression {
    pub base: BaseExpression,
    pub left: Box<Expression>,
    pub right: Box<Expression>,
}

#[derive(Debug, Clone)]
pub struct ConjunctionExpression {
    pub base: BaseExpression,
    pub children: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct ConstantExpression {
    pub base: BaseExpression,
    pub value: ConstantValue,
}

#[derive(Debug, Clone)]
pub struct ColumnRefExpression {
    pub base: BaseExpression,
    pub binding: ColumnBinding,
    pub return_type: Option<LogicalType>,
    pub depth: i32,
}

#[derive(Debug, Clone)]
pub struct FunctionExpression {
    pub base: BaseExpression,
    pub name: String,
    pub children: Vec<Expression>,
    pub return_type: Option<LogicalType>,
    pub catalog_name: String,
    pub schema_name: String,
    pub is_operator: bool,
}

#[derive(Debug, Clone)]
pub struct CastExpression {
    pub base: BaseExpression,
    pub child: Box<Expression>,
    pub return_type: Option<LogicalType>,
    pub try_cast: bool,
}

#[derive(Debug, Clone)]
pub struct BetweenExpression {
    pub base: BaseExpression,
    pub input: Box<Expression>,
    pub lower: Box<Expression>,
    pub upper: Box<Expression>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}

#[derive(Debug, Clone)]
pub struct OperatorExpression {
    pub base: BaseExpression,
    pub children: Vec<Expression>,
    pub return_type: Option<LogicalType>,
}

#[derive(Debug, Clone)]
pub struct CaseCheck {
    pub when_expr: Expression,
    pub then_expr: Expression,
}

#[derive(Debug, Clone)]
pub struct CaseExpression {
    pub base: BaseExpression,
    pub case_checks: Vec<CaseCheck>,
    pub else_expr: Option<Box<Expression>>,
    pub return_type: Option<LogicalType>,
}

#[derive(Debug, Clone)]
pub struct ParameterExpression {
    pub base: BaseExpression,
    pub identifier: String,
    pub return_type: Option<LogicalType>,
}

#[derive(Debug, Clone)]
pub struct ReferenceExpression {
    pub base: BaseExpression,
    pub return_type: Option<LogicalType>,
    pub index: i32,
}

#[derive(Debug, Clone)]
pub struct AggregateExpression {
    pub base: BaseExpression,
    pub name: String,
    pub children: Vec<Expression>,
    pub return_type: Option<LogicalType>,
}

#[derive(Debug, Clone)]
pub struct WindowExpression {
    pub base: BaseExpression,
    pub children: Vec<Expression>,
    pub partitions: Vec<Expression>,
    pub return_type: Option<LogicalType>,
    pub ignore_nulls: bool,
    pub distinct: bool,
}

#[derive(Debug, Clone)]
pub struct UnsupportedExpression {
    pub base: BaseExpression,
}
