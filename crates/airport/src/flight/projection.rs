use arrow_schema::{Field, Schema as ArrowSchema};
use std::sync::Arc;

/// Projects a schema to only include the specified columns.
pub fn project_schema(schema: &ArrowSchema, columns: &[&str]) -> Arc<ArrowSchema> {
    if columns.is_empty() {
        return Arc::new(schema.clone());
    }

    let col_index: std::collections::HashMap<&str, usize> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (f.name().as_str(), i))
        .collect();

    let fields: Vec<Field> = columns
        .iter()
        .filter_map(|col| col_index.get(*col).map(|&i| schema.field(i).clone()))
        .collect();

    if fields.is_empty() {
        return Arc::new(schema.clone());
    }

    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

/// Returns only the non-rowid columns from a schema.
pub fn non_rowid_columns(schema: &ArrowSchema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| {
            f.name() != "rowid"
                && f.metadata().get("is_rowid").map(|v| v.is_empty()).unwrap_or(true)
        })
        .map(|f| f.name().clone())
        .collect()
}
