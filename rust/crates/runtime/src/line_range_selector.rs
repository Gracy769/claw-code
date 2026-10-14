/// GJC-compatible line range selector parsing for the read tool.
///
/// Supports selector syntax:
/// - `:50` = start at line 50
/// - `:50-200` = lines 50-200 inclusive
/// - `:50+150` = 150 lines starting at 50
/// - `:50+1` = exactly one line
/// - `:5-16,960-973` = multiple ranges
/// - `:raw` = full verbatim
/// - `:conflicts` = git merge conflicts only
use std::num::ParseIntError;

/// Parsed line range specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LineRangeSelector {
    /// Full file verbatim (`:raw`)
    Raw,
    /// Git merge conflict sections only (`:conflicts`)
    Conflicts,
    /// Line range(s) (`:L-L+N` syntax)
    Lines(Vec<LineRange>),
    /// No selector (full file, may be truncated by default budget)
    None,
}

/// A single line range, zero-indexed internally but displayed as 1-indexed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineRange {
    /// Start line (1-indexed, user-facing)
    pub start_line: usize,
    /// End line inclusive (1-indexed, user-facing)
    pub end_line: usize,
}

impl LineRange {
    /// Create a new line range (1-indexed).
    pub fn new(start_line: usize, end_line: usize) -> Self {
        Self {
            start_line,
            end_line,
        }
    }

    /// Convert to zero-indexed range for internal use.
    pub fn zero_indexed(&self) -> (usize, usize) {
        (
            self.start_line.saturating_sub(1),
            self.end_line.saturating_sub(1),
        )
    }

    /// Get the number of lines in this range.
    pub fn len(&self) -> usize {
        if self.start_line <= self.end_line {
            self.end_line - self.start_line + 1
        } else {
            0
        }
    }

    /// Check if this range is valid.
    pub fn is_valid(&self) -> bool {
        self.start_line > 0 && self.end_line > 0 && self.start_line <= self.end_line
    }
}

/// Parse error types for line range selectors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    InvalidFormat(String),
    InvalidNumber(ParseIntError),
    InvalidRange(String),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidFormat(msg) => write!(f, "Invalid format: {}", msg),
            ParseError::InvalidNumber(e) => write!(f, "Invalid number: {}", e),
            ParseError::InvalidRange(msg) => write!(f, "Invalid range: {}", msg),
        }
    }
}

impl std::error::Error for ParseError {}

impl From<ParseIntError> for ParseError {
    fn from(e: ParseIntError) -> Self {
        ParseError::InvalidNumber(e)
    }
}

/// Parse a line range selector string.
///
/// Examples:
/// - `"50"` → `LineRangeSelector::Lines(vec![LineRange::new(50, 50)])`
/// - `"50-200"` → `LineRangeSelector::Lines(vec![LineRange::new(50, 200)])`
/// - `"50+150"` → `LineRangeSelector::Lines(vec![LineRange::new(50, 199)])`
/// - `"raw"` → `LineRangeSelector::Raw`
/// - `"conflicts"` → `LineRangeSelector::Conflicts`
pub fn parse_selector(selector: &str) -> Result<LineRangeSelector, ParseError> {
    if selector.is_empty() {
        return Ok(LineRangeSelector::None);
    }

    match selector {
        "raw" => Ok(LineRangeSelector::Raw),
        "conflicts" => Ok(LineRangeSelector::Conflicts),
        _ => parse_line_ranges(selector),
    }
}

/// Parse comma-separated line ranges.
fn parse_line_ranges(selector: &str) -> Result<LineRangeSelector, ParseError> {
    let mut ranges = Vec::new();

    for range_str in selector.split(',') {
        let range_str = range_str.trim();
        if range_str.is_empty() {
            continue;
        }

        let range = parse_single_range(range_str)?;
        ranges.push(range);
    }

    if ranges.is_empty() {
        Ok(LineRangeSelector::None)
    } else {
        // Merge overlapping/adjacent ranges
        ranges.sort_by_key(|r| r.start_line);
        let merged = merge_ranges(ranges);
        Ok(LineRangeSelector::Lines(merged))
    }
}

/// Parse a single line range in formats: "50", "50-200", "50+150".
fn parse_single_range(range_str: &str) -> Result<LineRange, ParseError> {
    // Try "50+150" format first (start+count)
    if let Some(plus_idx) = range_str.find('+') {
        let start_str = &range_str[..plus_idx];
        let count_str = &range_str[plus_idx + 1..];

        if start_str.is_empty() || count_str.is_empty() {
            return Err(ParseError::InvalidFormat(range_str.to_string()));
        }

        let start = start_str.parse::<usize>()?;
        let count = count_str.parse::<usize>()?;

        if start == 0 || count == 0 {
            return Err(ParseError::InvalidRange(
                "Line numbers and counts must be >= 1".to_string(),
            ));
        }

        return Ok(LineRange::new(start, start + count - 1));
    }

    // Try "50-200" format (start-end)
    if let Some(dash_idx) = range_str.find('-') {
        // Skip leading dash (negative numbers aren't valid here)
        if dash_idx == 0 {
            return Err(ParseError::InvalidFormat(range_str.to_string()));
        }

        let start_str = &range_str[..dash_idx];
        let end_str = &range_str[dash_idx + 1..];

        if start_str.is_empty() || end_str.is_empty() {
            return Err(ParseError::InvalidFormat(range_str.to_string()));
        }

        let start = start_str.parse::<usize>()?;
        let end = end_str.parse::<usize>()?;

        if start == 0 || end == 0 {
            return Err(ParseError::InvalidRange(
                "Line numbers must be >= 1".to_string(),
            ));
        }

        if start > end {
            return Err(ParseError::InvalidRange(format!(
                "Start line {} cannot be greater than end line {}",
                start, end
            )));
        }

        return Ok(LineRange::new(start, end));
    }

    // Try plain "50" format (single line)
    let line_num = range_str.parse::<usize>()?;
    if line_num == 0 {
        return Err(ParseError::InvalidRange(
            "Line number must be >= 1".to_string(),
        ));
    }

    Ok(LineRange::new(line_num, line_num))
}

/// Merge overlapping and adjacent ranges.
fn merge_ranges(ranges: Vec<LineRange>) -> Vec<LineRange> {
    let mut merged: Vec<LineRange> = Vec::new();

    for range in ranges {
        if let Some(last) = merged.last_mut() {
            // If ranges overlap or are adjacent, merge them
            if range.start_line <= last.end_line + 1 {
                last.end_line = last.end_line.max(range.end_line);
            } else {
                merged.push(range);
            }
        } else {
            merged.push(range);
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_raw() {
        assert_eq!(parse_selector("raw").unwrap(), LineRangeSelector::Raw);
    }

    #[test]
    fn test_parse_conflicts() {
        assert_eq!(
            parse_selector("conflicts").unwrap(),
            LineRangeSelector::Conflicts
        );
    }

    #[test]
    fn test_parse_single_line() {
        match parse_selector("50").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start_line, 50);
                assert_eq!(ranges[0].end_line, 50);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_parse_range_start_dash_end() {
        match parse_selector("50-200").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start_line, 50);
                assert_eq!(ranges[0].end_line, 200);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_parse_range_start_plus_count() {
        match parse_selector("50+150").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start_line, 50);
                assert_eq!(ranges[0].end_line, 199);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_parse_multiple_ranges() {
        match parse_selector("5-16,960-973").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 2);
                assert_eq!(ranges[0].start_line, 5);
                assert_eq!(ranges[0].end_line, 16);
                assert_eq!(ranges[1].start_line, 960);
                assert_eq!(ranges[1].end_line, 973);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_merge_overlapping_ranges() {
        // "5-16,10-20" should merge into "5-20"
        match parse_selector("5-16,10-20").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start_line, 5);
                assert_eq!(ranges[0].end_line, 20);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_merge_adjacent_ranges() {
        // "5-16,17-20" should merge into "5-20"
        match parse_selector("5-16,17-20").unwrap() {
            LineRangeSelector::Lines(ranges) => {
                assert_eq!(ranges.len(), 1);
                assert_eq!(ranges[0].start_line, 5);
                assert_eq!(ranges[0].end_line, 20);
            }
            _ => panic!("Expected Lines variant"),
        }
    }

    #[test]
    fn test_invalid_range_start_greater_than_end() {
        let result = parse_selector("200-50");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_zero_line() {
        let result = parse_selector("0");
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_selector() {
        assert_eq!(parse_selector("").unwrap(), LineRangeSelector::None);
    }

    #[test]
    fn test_line_range_zero_indexed() {
        let range = LineRange::new(50, 200);
        let (start, end) = range.zero_indexed();
        assert_eq!(start, 49);
        assert_eq!(end, 199);
    }

    #[test]
    fn test_line_range_len() {
        assert_eq!(LineRange::new(50, 200).len(), 151);
        assert_eq!(LineRange::new(50, 50).len(), 1);
    }
}
