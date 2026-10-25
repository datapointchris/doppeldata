//! Reads a profile's `physical_type`, an Arrow type as pyarrow writes it, such as `int64`,
//! `decimal128(12, 2)` or `timestamp[us, tz=UTC]`. A list or struct type is read only by its
//! container name, because its children carry their own types.

use arrow_schema::DataType;
use arrow_schema::TimeUnit;

/// A physical type that is a container, whose children decide the full type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Container {
    List,
    LargeList,
    Struct,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum PhysicalType {
    Scalar(DataType),
    Container(Container),
}

pub(crate) fn parse(text: &str) -> Option<PhysicalType> {
    let text = text.trim();
    let head = text.split(['<', '[', '(']).next().unwrap_or(text).trim();
    let container = match head {
        "list" => Some(Container::List),
        "large_list" => Some(Container::LargeList),
        "struct" => Some(Container::Struct),
        _ => None,
    };
    if let Some(container) = container {
        return Some(PhysicalType::Container(container));
    }
    scalar(text).map(PhysicalType::Scalar)
}

fn scalar(text: &str) -> Option<DataType> {
    let simple = match text {
        "bool" | "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float" | "float32" => DataType::Float32,
        "double" | "float64" => DataType::Float64,
        "string" | "str" | "utf8" => DataType::Utf8,
        "large_string" | "large_str" | "large_utf8" => DataType::LargeUtf8,
        "date32" | "date32[day]" => DataType::Date32,
        "date64" | "date64[ms]" => DataType::Date64,
        _ => return parameterized(text),
    };
    Some(simple)
}

fn parameterized(text: &str) -> Option<DataType> {
    if let Some(args) = text.strip_prefix("decimal128(").and_then(|rest| rest.strip_suffix(')')) {
        let (precision, scale) = args.split_once(',')?;
        return Some(DataType::Decimal128(precision.trim().parse().ok()?, scale.trim().parse().ok()?));
    }
    if let Some(args) = text.strip_prefix("timestamp[").and_then(|rest| rest.strip_suffix(']')) {
        let mut parts = args.split(',').map(str::trim);
        let unit = time_unit(parts.next()?)?;
        let timezone = match parts.next() {
            Some(tz) => Some(tz.strip_prefix("tz=")?.into()),
            None => None,
        };
        return Some(DataType::Timestamp(unit, timezone));
    }
    if let Some(unit) = text.strip_prefix("time32[").and_then(|rest| rest.strip_suffix(']')) {
        return match time_unit(unit)? {
            unit @ (TimeUnit::Second | TimeUnit::Millisecond) => Some(DataType::Time32(unit)),
            _ => None,
        };
    }
    if let Some(unit) = text.strip_prefix("time64[").and_then(|rest| rest.strip_suffix(']')) {
        return match time_unit(unit)? {
            unit @ (TimeUnit::Microsecond | TimeUnit::Nanosecond) => Some(DataType::Time64(unit)),
            _ => None,
        };
    }
    None
}

/// `data_type` as pyarrow writes it, the spelling a profile's `physical_type` takes, such as
/// `timestamp[us, tz=UTC]` or `list<item: int64>`. A type pyarrow has no other name for is written
/// as arrow-rs names it.
pub fn physical_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "bool".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 => "string".into(),
        DataType::LargeUtf8 => "large_string".into(),
        DataType::Date32 => "date32[day]".into(),
        DataType::Date64 => "date64[ms]".into(),
        DataType::Decimal128(precision, scale) => format!("decimal128({precision}, {scale})"),
        DataType::Timestamp(unit, None) => format!("timestamp[{}]", unit_name(*unit)),
        DataType::Timestamp(unit, Some(tz)) => format!("timestamp[{}, tz={tz}]", unit_name(*unit)),
        DataType::Time32(unit) => format!("time32[{}]", unit_name(*unit)),
        DataType::Time64(unit) => format!("time64[{}]", unit_name(*unit)),
        DataType::List(item) => format!("list<{}: {}>", item.name(), physical_type_name(item.data_type())),
        DataType::LargeList(item) => format!("large_list<{}: {}>", item.name(), physical_type_name(item.data_type())),
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), physical_type_name(field.data_type())))
                .collect();
            format!("struct<{}>", fields.join(", "))
        }
        other => other.to_string(),
    }
}

fn unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}

fn time_unit(text: &str) -> Option<TimeUnit> {
    Some(match text {
        "s" => TimeUnit::Second,
        "ms" => TimeUnit::Millisecond,
        "us" => TimeUnit::Microsecond,
        "ns" => TimeUnit::Nanosecond,
        _ => return None,
    })
}
