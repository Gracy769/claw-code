use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonError {
    message: String,
}

impl JsonError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl Display for JsonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for JsonError {}

impl JsonValue {
    #[must_use]
    pub fn render(&self) -> String {
        match self {
            Self::Null => "null".to_string(),
            Self::Bool(value) => value.to_string(),
            Self::Number(value) => value.to_string(),
            Self::String(value) => render_string(value),
            Self::Array(values) => {
                let rendered = values
                    .iter()
                    .map(Self::render)
                    .collect::<Vec<_>>()
                    .join(",");
                format!("[{rendered}]")
            }
            Self::Object(map) => {
                let rendered = map
                    .iter()
                    .map(|(k, v)| format!("{}:{}", render_string(k), v.render()))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{{{rendered}}}")
            }
        }
    }

    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Number(n) => Some(*n),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) => Some(s),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_array(&self) -> Option<&[JsonValue]> {
        match self {
            Self::Array(a) => Some(a),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_object(&self) -> Option<&BTreeMap<String, JsonValue>> {
        match self {
            Self::Object(o) => Some(o),
            _ => None,
        }
    }

    pub fn parse(input: &str) -> Result<Self, JsonError> {
        let mut chars = input.trim().chars().peekable();
        let value = parse_value(&mut chars)?;
        if chars.peek().is_some() {
            return Err(JsonError::new("trailing content"));
        }
        Ok(value)
    }
}

fn parse_value(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    match chars.peek() {
        Some('n') => parse_null(chars),
        Some('t') | Some('f') => parse_bool(chars),
        Some('"') => parse_string(chars).map(JsonValue::String),
        Some('[') => parse_array(chars),
        Some('{') => parse_object(chars),
        Some('-') | Some('0'..='9') => parse_number(chars),
        Some(c) => Err(JsonError::new(format!("unexpected char: {c}"))),
        None => Err(JsonError::new("empty input")),
    }
}

fn parse_null(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    if consume_literal(chars, "null") {
        Ok(JsonValue::Null)
    } else {
        Err(JsonError::new("invalid null"))
    }
}

fn parse_bool(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    if consume_literal(chars, "true") {
        return Ok(JsonValue::Bool(true));
    }
    if consume_literal(chars, "false") {
        return Ok(JsonValue::Bool(false));
    }
    Err(JsonError::new("invalid bool"))
}

fn parse_string(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<String, JsonError> {
    if chars.next() != Some('"') {
        return Err(JsonError::new("expected opening quote"));
    }

    let mut result = String::new();
    loop {
        match chars.next() {
            None => return Err(JsonError::new("unterminated string")),
            Some('"') => return Ok(result),
            Some('\\') => match chars.next() {
                Some('"') => result.push('"'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some('b') => result.push('\u{08}'),
                Some('f') => result.push('\u{0C}'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('u') => {
                    let mut hex = String::new();
                    for _ in 0..4 {
                        match chars.next() {
                            Some(c @ '0'..='9') | Some(c @ 'a'..='f') | Some(c @ 'A'..='F') => {
                                hex.push(c)
                            }
                            _ => return Err(JsonError::new("invalid unicode escape")),
                        }
                    }
                    let code = u32::from_str_radix(&hex, 16)
                        .map_err(|_| JsonError::new("invalid hex code"))?;
                    if (0xD800..=0xDFFF).contains(&code) {
                        return Err(JsonError::new("surrogate not allowed"));
                    }
                    result.push(
                        char::from_u32(code).ok_or_else(|| JsonError::new("invalid codepoint"))?,
                    );
                }
                _ => return Err(JsonError::new("invalid escape")),
            },
            Some(c) => result.push(c),
        }
    }
}

fn parse_number(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    let mut num_str = String::new();

    if chars.peek() == Some(&'-') {
        num_str.push(chars.next().unwrap());
    }

    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            num_str.push(chars.next().unwrap());
        } else {
            break;
        }
    }

    if num_str.is_empty() || num_str == "-" {
        return Err(JsonError::new("invalid number"));
    }

    num_str
        .parse::<i64>()
        .map(JsonValue::Number)
        .map_err(|_| JsonError::new("number out of range"))
}

fn parse_array(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    if chars.next() != Some('[') {
        return Err(JsonError::new("expected ["));
    }

    let mut values = Vec::new();

    skip_whitespace(chars);
    if chars.peek() == Some(&']') {
        chars.next();
        return Ok(JsonValue::Array(values));
    }

    loop {
        skip_whitespace(chars);
        values.push(parse_value(chars)?);
        skip_whitespace(chars);

        match chars.peek() {
            Some(',') => {
                chars.next();
            }
            Some(']') => {
                chars.next();
                return Ok(JsonValue::Array(values));
            }
            _ => return Err(JsonError::new("expected , or ]")),
        }
    }
}

fn parse_object(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<JsonValue, JsonError> {
    if chars.next() != Some('{') {
        return Err(JsonError::new("expected {"));
    }

    let mut map = BTreeMap::new();

    skip_whitespace(chars);
    if chars.peek() == Some(&'}') {
        chars.next();
        return Ok(JsonValue::Object(map));
    }

    loop {
        skip_whitespace(chars);

        if chars.peek() != Some(&'"') {
            return Err(JsonError::new("expected key string"));
        }
        let key = parse_string(chars)?;

        skip_whitespace(chars);
        if chars.next() != Some(':') {
            return Err(JsonError::new("expected :"));
        }

        skip_whitespace(chars);
        let value = parse_value(chars)?;
        map.insert(key, value);

        skip_whitespace(chars);
        match chars.peek() {
            Some(',') => {
                chars.next();
            }
            Some('}') => {
                chars.next();
                return Ok(JsonValue::Object(map));
            }
            _ => return Err(JsonError::new("expected , or }")),
        }
    }
}

fn consume_literal(chars: &mut std::iter::Peekable<std::str::Chars>, literal: &str) -> bool {
    let remaining: String = chars.clone().collect();
    if remaining.starts_with(literal) {
        for _ in literal.chars() {
            chars.next();
        }
        true
    } else {
        false
    }
}

fn skip_whitespace(chars: &mut std::iter::Peekable<std::str::Chars>) {
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else {
            break;
        }
    }
}

fn render_string(s: &str) -> String {
    let mut result = String::from('"');
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            '\u{08}' => result.push_str("\\b"),
            '\u{0C}' => result.push_str("\\f"),
            c if c.is_control() => {
                let code = c as u32;
                result.push_str(&format!("\\u{code:04x}"));
            }
            c => result.push(c),
        }
    }
    result.push('"');
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_primitive_values() {
        assert_eq!(JsonValue::Null.render(), "null");
        assert_eq!(JsonValue::Bool(true).render(), "true");
        assert_eq!(JsonValue::Bool(false).render(), "false");
        assert_eq!(JsonValue::Number(42).render(), "42");
        assert_eq!(JsonValue::Number(-7).render(), "-7");
        assert_eq!(JsonValue::String("hi".to_string()).render(), "\"hi\"");
    }

    #[test]
    fn renders_arrays_and_objects_with_sorted_keys() {
        assert_eq!(JsonValue::Array(Vec::new()).render(), "[]");
        assert_eq!(
            JsonValue::Array(vec![JsonValue::Number(1), JsonValue::Number(2)]).render(),
            "[1,2]"
        );
        assert_eq!(JsonValue::Object(BTreeMap::new()).render(), "{}");

        let mut object = BTreeMap::new();
        object.insert("b".to_string(), JsonValue::Bool(false));
        object.insert("a".to_string(), JsonValue::Number(1));
        // A BTreeMap iterates keys in sorted order, so object rendering is
        // deterministic regardless of insertion order.
        assert_eq!(JsonValue::Object(object).render(), "{\"a\":1,\"b\":false}");
    }

    #[test]
    fn render_string_escapes_quotes_backslashes_and_other_controls() {
        // Backslash and double-quote are backslash-escaped.
        assert_eq!(render_string("\"\\"), "\"\\\"\\\\\"");
        // Carriage return has a dedicated short escape.
        assert_eq!(render_string("\r"), "\"\\r\"");
        // Other control characters fall back to the \uXXXX form (U+0001 here).
        assert_eq!(render_string("\u{1}"), "\"\\u0001\"");
    }

    #[test]
    fn parses_primitive_values() {
        assert_eq!(
            JsonValue::parse("null").expect("null parses"),
            JsonValue::Null
        );
        assert_eq!(
            JsonValue::parse("true").expect("true parses"),
            JsonValue::Bool(true)
        );
        assert_eq!(
            JsonValue::parse("false").expect("false parses"),
            JsonValue::Bool(false)
        );
        assert_eq!(
            JsonValue::parse("\"hello\"").expect("string parses"),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn parses_signed_integers_and_ignores_surrounding_whitespace() {
        assert_eq!(
            JsonValue::parse("-42").expect("negative parses").as_i64(),
            Some(-42)
        );
        assert_eq!(
            JsonValue::parse("  123  ").expect("padded parses").as_i64(),
            Some(123)
        );
    }

    #[test]
    fn parses_nested_arrays_and_objects() {
        let value =
            JsonValue::parse("{\"nums\": [1, 2], \"flag\": true}").expect("nested json parses");
        let object = value.as_object().expect("top level is an object");

        assert_eq!(object.len(), 2);

        let nums = object
            .get("nums")
            .and_then(JsonValue::as_array)
            .expect("nums is an array");
        assert_eq!(nums.len(), 2);
        assert_eq!(nums[0].as_i64(), Some(1));

        assert_eq!(object.get("flag").and_then(JsonValue::as_bool), Some(true));
    }

    #[test]
    fn round_trips_rendered_values_through_the_parser() {
        let mut object = BTreeMap::new();
        object.insert("name".to_string(), JsonValue::String("clawi".to_string()));
        object.insert("count".to_string(), JsonValue::Number(-3));
        object.insert(
            "tags".to_string(),
            JsonValue::Array(vec![JsonValue::Bool(true), JsonValue::Null]),
        );
        let original = JsonValue::Object(object);

        let reparsed = JsonValue::parse(&original.render()).expect("rendered json should re-parse");

        assert_eq!(reparsed, original);
    }

    #[test]
    fn escapes_control_characters() {
        assert_eq!(render_string("a\n\t\"b"), "\"a\\n\\t\\\"b\"");
    }

    #[test]
    fn parses_escape_sequences_inside_strings() {
        // Short escapes must decode to the control character they name rather
        // than to the literal letter that follows the backslash.
        assert_eq!(
            JsonValue::parse("\"a\\nb\"").expect("newline escape parses"),
            JsonValue::String("a\nb".to_string())
        );
        assert_eq!(
            JsonValue::parse("\"a\\tb\"").expect("tab escape parses"),
            JsonValue::String("a\tb".to_string())
        );
        assert_eq!(
            JsonValue::parse("\"\\r\\b\\f\"").expect("control escapes parse"),
            JsonValue::String("\r\u{08}\u{0C}".to_string())
        );
        // JSON allows a forward slash to be escaped even though it need not be.
        assert_eq!(
            JsonValue::parse("\"a\\/b\"").expect("escaped slash parses"),
            JsonValue::String("a/b".to_string())
        );
        // An escaped quote must not terminate the string, and an escaped
        // backslash must not start another escape sequence.
        assert_eq!(
            JsonValue::parse("\"\\\"\\\\\"").expect("escaped quote and backslash parse"),
            JsonValue::String("\"\\".to_string())
        );
    }

    #[test]
    fn parses_four_digit_unicode_escapes() {
        // All four hex digits belong to the escape: stopping after three would
        // leave the fourth as a literal character.
        assert_eq!(
            JsonValue::parse("\"\\u0041\"").expect("four digit escape parses"),
            JsonValue::String("A".to_string())
        );
        // Hex digits are accepted in either case.
        assert_eq!(
            JsonValue::parse("\"\\u00E9\"").expect("uppercase hex digits parse"),
            JsonValue::String("\u{e9}".to_string())
        );
        assert_eq!(
            JsonValue::parse("\"\\u00e9\"").expect("lowercase hex digits parse"),
            JsonValue::String("\u{e9}".to_string())
        );
        // Fewer than four hex digits is not a unicode escape.
        assert!(JsonValue::parse("\"\\u00e\"").is_err(), "three hex digits");
        assert!(JsonValue::parse("\"\\uZZZZ\"").is_err(), "non hex digits");
        // A surrogate is not a unicode scalar value, so it cannot be a char.
        assert!(JsonValue::parse("\"\\ud800\"").is_err(), "lone surrogate");
    }

    #[test]
    fn round_trips_parsed_escapes_through_the_renderer() {
        // Every escape that parses must render back to JSON that re-parses to
        // the same value. The renderer may pick an equivalent spelling (for
        // example \u000a instead of \n), so only the round trip is asserted.
        for source in [
            "\"a\\nb\"",
            "\"a\\tb\"",
            "\"a\\/b\"",
            "\"\\r\\b\\f\"",
            "\"\\\"\\\\\"",
            "\"\\u0041\"",
            "\"\\u00E9\"",
        ] {
            let parsed = JsonValue::parse(source).expect("escaped source parses");
            let reparsed = JsonValue::parse(&parsed.render()).expect("rendered value re-parses");
            assert_eq!(reparsed, parsed, "round trip changed {source}");
        }

        // Object keys are escaped on the way out and unescaped on the way in.
        let mut object = BTreeMap::new();
        object.insert("a\nb".to_string(), JsonValue::String("c\td".to_string()));
        let original = JsonValue::Object(object);
        let reparsed = JsonValue::parse(&original.render()).expect("escaped key re-parses");

        assert_eq!(reparsed, original);
    }

    #[test]
    fn parses_the_i64_boundary_and_rejects_the_values_past_it() {
        // i64::MAX and i64::MIN are representable, so they must parse to the
        // exact value rather than being narrowed or clamped.
        assert_eq!(
            JsonValue::parse("9223372036854775807")
                .expect("i64::MAX parses")
                .as_i64(),
            Some(i64::MAX)
        );
        assert_eq!(
            JsonValue::parse("-9223372036854775808")
                .expect("i64::MIN parses")
                .as_i64(),
            Some(i64::MIN)
        );
        assert_eq!(
            JsonValue::parse("-9223372036854775807")
                .expect("i64::MIN + 1 parses")
                .as_i64(),
            Some(i64::MIN + 1)
        );

        // One past either end of the range must be rejected. This is what pins
        // the accepted range at i64: a rejection test built from an obviously
        // huge number is also satisfied by a much narrower integer type.
        assert!(
            JsonValue::parse("9223372036854775808").is_err(),
            "i64::MAX + 1"
        );
        assert!(
            JsonValue::parse("-9223372036854775809").is_err(),
            "i64::MIN - 1"
        );
    }

    #[test]
    fn parse_rejects_malformed_input() {
        assert!(JsonValue::parse("").is_err(), "empty input");
        assert!(JsonValue::parse("nul").is_err(), "truncated literal");
        assert!(JsonValue::parse("true false").is_err(), "trailing content");
        assert!(
            JsonValue::parse("\"unterminated").is_err(),
            "unterminated string"
        );
        assert!(JsonValue::parse("[1, 2").is_err(), "unterminated array");
    }

    #[test]
    fn parse_rejects_integers_outside_the_i64_range() {
        // Far beyond i64::MAX (~9.2e18): the parser must reject rather than
        // silently truncate the value.
        assert!(JsonValue::parse("9999999999999999999999999").is_err());
    }

    #[test]
    fn accessors_return_none_for_mismatched_variants() {
        assert_eq!(JsonValue::Null.as_bool(), None);
        assert_eq!(JsonValue::Bool(true).as_i64(), None);
        assert!(JsonValue::Number(1).as_str().is_none());
        assert!(JsonValue::Number(1).as_array().is_none());
        assert!(JsonValue::String("x".to_string()).as_object().is_none());
    }

    #[test]
    fn json_error_displays_its_message() {
        assert_eq!(JsonError::new("boom").to_string(), "boom");
    }
}
