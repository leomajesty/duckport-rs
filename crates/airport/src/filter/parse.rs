use super::types::*;
use serde_json::Value as JsonValue;

/// Parses a JSON filter predicate from DuckDB into a FilterPushdown structure.
pub fn parse(data: &[u8]) -> Option<FilterPushdown> {
    if data.is_empty() {
        return None;
    }
    let json: JsonValue = serde_json::from_slice(data).ok()?;
    let obj = json.as_object()?;

    // Extract column bindings
    let column_bindings: Vec<String> = obj
        .get("column_binding_names_by_index")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Parse filters array
    let filters: Vec<Expression> = obj
        .get("filters")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| parse_expression(v))
                .collect()
        })
        .unwrap_or_default();

    Some(FilterPushdown {
        filters,
        column_bindings,
    })
}

/// Parses a single expression from JSON.
pub fn parse_expression(value: &JsonValue) -> Option<Expression> {
    let obj = value.as_object()?;
    let class_str = obj.get("expression_class")?.as_str()?;
    let type_str = obj.get("type")?.as_str()?;

    let base = BaseExpression {
        expression_class: parse_expression_class(class_str),
        expr_type: parse_expression_type(type_str),
        alias: obj
            .get("alias")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string(),
    };

    match class_str {
        "BOUND_COMPARISON" => parse_comparison(obj, base),
        "BOUND_CONJUNCTION" => parse_conjunction(obj, base),
        "BOUND_CONSTANT" => parse_constant(obj, base),
        "BOUND_COLUMN_REF" => parse_column_ref(obj, base),
        "BOUND_FUNCTION" => parse_function(obj, base),
        "BOUND_CAST" => parse_cast(obj, base),
        "BOUND_BETWEEN" => parse_between(obj, base),
        "BOUND_OPERATOR" => parse_operator(obj, base),
        "BOUND_CASE" => parse_case(obj, base),
        "BOUND_PARAMETER" => parse_parameter(obj, base),
        "BOUND_REF" => parse_reference(obj, base),
        "BOUND_AGGREGATE" => parse_aggregate(obj, base),
        "BOUND_WINDOW" => parse_window(obj, base),
        _ => Some(Expression::Unsupported(UnsupportedExpression { base })),
    }
}

fn parse_expression_class(s: &str) -> ExpressionClass {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .unwrap_or(ExpressionClass::Unknown)
}

fn parse_expression_type(s: &str) -> ExpressionType {
    serde_json::from_value(serde_json::Value::String(s.to_string()))
        .unwrap_or(ExpressionType::Unknown)
}

fn parse_logical_type(value: &JsonValue) -> Option<LogicalType> {
    serde_json::from_value(value.clone()).ok()
}

fn parse_comparison(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let left = parse_expression(obj.get("left")?)?;
    let right = parse_expression(obj.get("right")?)?;
    Some(Expression::Comparison(ComparisonExpression {
        base,
        left: Box::new(left),
        right: Box::new(right),
    }))
}

fn parse_conjunction(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let children = obj
        .get("children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    Some(Expression::Conjunction(ConjunctionExpression {
        base,
        children,
    }))
}

fn parse_constant(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let value_json = obj.get("value")?;
    let value_type = parse_logical_type(value_json.get("type")?)?;
    let value = value_json.get("value").cloned().unwrap_or(JsonValue::Null);
    let is_null = value_json
        .get("is_null")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(Expression::Constant(ConstantExpression {
        base,
        value: ConstantValue {
            value_type,
            value,
            is_null,
        },
    }))
}

fn parse_column_ref(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let binding_json = obj.get("binding")?;
    let table_index = binding_json
        .get("table_index")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let column_index = binding_json
        .get("column_index")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    let depth = obj
        .get("depth")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    Some(Expression::ColumnRef(ColumnRefExpression {
        base,
        binding: ColumnBinding {
            table_index,
            column_index,
        },
        return_type,
        depth,
    }))
}

fn parse_function(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let name = obj
        .get("function_name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let children = obj
        .get("children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    let catalog_name = obj
        .get("catalog")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let schema_name = obj
        .get("schema")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let is_operator = obj
        .get("is_operator")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(Expression::Function(FunctionExpression {
        base,
        name,
        children,
        return_type,
        catalog_name,
        schema_name,
        is_operator,
    }))
}

fn parse_cast(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let child = parse_expression(obj.get("child")?)?;
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    let try_cast = obj
        .get("try_cast")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(Expression::Cast(CastExpression {
        base,
        child: Box::new(child),
        return_type,
        try_cast,
    }))
}

fn parse_between(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let input = parse_expression(obj.get("input")?)?;
    let lower = parse_expression(obj.get("lower")?)?;
    let upper = parse_expression(obj.get("upper")?)?;
    let lower_inclusive = obj
        .get("lower_inclusive")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let upper_inclusive = obj
        .get("upper_inclusive")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    Some(Expression::Between(BetweenExpression {
        base,
        input: Box::new(input),
        lower: Box::new(lower),
        upper: Box::new(upper),
        lower_inclusive,
        upper_inclusive,
    }))
}

fn parse_operator(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let children = obj
        .get("children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    Some(Expression::Operator(OperatorExpression {
        base,
        children,
        return_type,
    }))
}

fn parse_case(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let case_checks = obj
        .get("case_checks")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|check| {
                    let when_expr = parse_expression(check.get("when_expr")?)?;
                    let then_expr = parse_expression(check.get("then_expr")?)?;
                    Some(CaseCheck { when_expr, then_expr })
                })
                .collect()
        })
        .unwrap_or_default();
    let else_expr = obj
        .get("else_expr")
        .and_then(parse_expression)
        .map(Box::new);
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    Some(Expression::Case(CaseExpression {
        base,
        case_checks,
        else_expr,
        return_type,
    }))
}

fn parse_parameter(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let identifier = obj
        .get("identifier")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    Some(Expression::Parameter(ParameterExpression {
        base,
        identifier,
        return_type,
    }))
}

fn parse_reference(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let index = obj
        .get("index")
        .and_then(|v| v.as_i64())
        .unwrap_or(0) as i32;
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    Some(Expression::Reference(ReferenceExpression {
        base,
        return_type,
        index,
    }))
}

fn parse_aggregate(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let name = obj
        .get("aggregate_name")
        .or_else(|| obj.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let children = obj
        .get("children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    Some(Expression::Aggregate(AggregateExpression {
        base,
        name,
        children,
        return_type,
    }))
}

fn parse_window(
    obj: &serde_json::Map<String, JsonValue>,
    base: BaseExpression,
) -> Option<Expression> {
    let children = obj
        .get("children")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    let partitions = obj
        .get("partitions")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(parse_expression).collect())
        .unwrap_or_default();
    let return_type = obj.get("return_type").and_then(parse_logical_type);
    let ignore_nulls = obj
        .get("ignore_nulls")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let distinct = obj
        .get("distinct")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some(Expression::Window(WindowExpression {
        base,
        children,
        partitions,
        return_type,
        ignore_nulls,
        distinct,
    }))
}
