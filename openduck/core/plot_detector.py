"""Plot detector heuristics and plotext chart generation."""

from collections import Counter
import csv
from dataclasses import dataclass, field
import datetime
import decimal
import io
import re
from typing import Any, List, Optional, Sequence, Tuple

AVAILABLE_CHART_TYPES = ("bar", "line", "scatter", "hist", "count")

DATE_PATTERN = re.compile(
    r"^\d{4}[-/]\d{2}[-/]\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?$"
)


def is_numeric_value(val: Any) -> bool:
    """Check if a value is numeric (int, float, Decimal, or numeric string)."""
    if val is None or isinstance(val, bool):
        return False
    if isinstance(val, (int, float, decimal.Decimal)):
        return True
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return False
        try:
            float(s)
            return True
        except ValueError:
            return False
    return False


def is_temporal_value(val: Any) -> bool:
    """Check if a value is a date, datetime, or date-like string."""
    if val is None:
        return False
    if isinstance(val, (datetime.date, datetime.datetime, datetime.time)):
        return True
    if isinstance(val, str):
        return bool(DATE_PATTERN.match(val.strip()))
    return False


@dataclass
class ColumnProfile:
    """Profile of a single dataset column."""

    name: str
    index: int
    data_type: str  # "numeric", "temporal", "categorical"
    distinct_count: int = 0
    total_count: int = 0
    non_null_count: int = 0


@dataclass
class PlotSuggestion:
    """A recommended plot configuration."""

    chart_type: str  # "bar", "line", "scatter", "hist", "count"
    x_col: Optional[str]
    y_col: Optional[str] = None
    y_cols: Optional[List[str]] = None
    title: str = ""
    description: str = ""
    confidence: float = 1.0


def detect_column_types(
    column_names: Sequence[str], data: Sequence[Sequence[Any]], sample_limit: int = 300
) -> List[ColumnProfile]:
    """Analyze dataset columns and profile their data types.

    Args:
        column_names: List of column names.
        data: Rows of table data.
        sample_limit: Max number of rows to sample for profiling.

    Returns:
        List of ColumnProfile objects.
    """
    profiles: List[ColumnProfile] = []
    if not column_names:
        return profiles

    sample_rows = data[:sample_limit] if data else []
    total_rows = len(sample_rows)

    for idx, col_name in enumerate(column_names):
        col_values = [row[idx] for row in sample_rows if idx < len(row)]
        non_nulls = [v for v in col_values if v is not None and v != ""]
        non_null_count = len(non_nulls)
        distinct_count = len(set(non_nulls)) if non_nulls else 0

        if non_null_count == 0:
            col_type = "categorical"
        else:
            num_count = sum(1 for v in non_nulls if is_numeric_value(v))
            temp_count = sum(1 for v in non_nulls if is_temporal_value(v))

            if num_count / non_null_count >= 0.8:
                col_type = "numeric"
            elif temp_count / non_null_count >= 0.8:
                col_type = "temporal"
            else:
                col_type = "categorical"

        profiles.append(
            ColumnProfile(
                name=col_name,
                index=idx,
                data_type=col_type,
                distinct_count=distinct_count,
                total_count=total_rows,
                non_null_count=non_null_count,
            )
        )

    return profiles


def recommend_plots(
    column_names: Sequence[str], data: Sequence[Sequence[Any]]
) -> List[PlotSuggestion]:
    """Recommend chart types and column mappings based on heuristics.

    Supported chart types:
        - 'bar': categorical/temporal X, numeric Y
        - 'line': temporal/numeric X, numeric Y
        - 'scatter': numeric X, numeric Y
        - 'hist': single numeric column
        - 'count': single categorical column

    Returns:
        List of PlotSuggestion objects sorted by confidence (highest first).
    """
    if not column_names or not data:
        return []

    profiles = detect_column_types(column_names, data)
    num_cols = [p for p in profiles if p.data_type == "numeric"]
    temp_cols = [p for p in profiles if p.data_type == "temporal"]
    cat_cols = [p for p in profiles if p.data_type == "categorical"]

    suggestions: List[PlotSuggestion] = []

    # Case 1: Single column dataset
    if len(profiles) == 1:
        col = profiles[0]
        if col.data_type == "numeric":
            suggestions.append(
                PlotSuggestion(
                    chart_type="hist",
                    x_col=col.name,
                    y_col=None,
                    title=f"Distribution of {col.name}",
                    description=f"Histogram showing numeric distribution of {col.name}",
                    confidence=0.95,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="line",
                    x_col=None,
                    y_col=col.name,
                    title=f"{col.name} over rows",
                    description=f"Line plot of {col.name}",
                    confidence=0.75,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="bar",
                    x_col=None,
                    y_col=col.name,
                    title=f"{col.name} values",
                    description=f"Bar chart of {col.name}",
                    confidence=0.70,
                )
            )
        else:
            suggestions.append(
                PlotSuggestion(
                    chart_type="count",
                    x_col=col.name,
                    y_col=None,
                    title=f"Counts of {col.name}",
                    description=f"Frequency count of distinct categories in {col.name}",
                    confidence=0.95,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="bar",
                    x_col=col.name,
                    y_col=None,
                    title=f"{col.name} bar chart",
                    description=f"Bar chart for {col.name}",
                    confidence=0.65,
                )
            )
        return suggestions

    # Case 2: Temporal column + Numeric column(s) -> Line chart primary
    if temp_cols and num_cols:
        t_col = temp_cols[0]
        n_col = num_cols[0]
        suggestions.append(
            PlotSuggestion(
                chart_type="line",
                x_col=t_col.name,
                y_col=n_col.name,
                title=f"{n_col.name} over {t_col.name}",
                description=f"Time-series line plot of {n_col.name} across {t_col.name}",
                confidence=0.95,
            )
        )
        suggestions.append(
            PlotSuggestion(
                chart_type="bar",
                x_col=t_col.name,
                y_col=n_col.name,
                title=f"{n_col.name} by {t_col.name}",
                description=f"Bar chart of {n_col.name} by {t_col.name}",
                confidence=0.85,
            )
        )
        if len(num_cols) >= 2:
            suggestions.append(
                PlotSuggestion(
                    chart_type="scatter",
                    x_col=num_cols[0].name,
                    y_col=num_cols[1].name,
                    title=f"{num_cols[1].name} vs {num_cols[0].name}",
                    description="Scatter plot comparing two numeric columns",
                    confidence=0.80,
                )
            )

    # Case 3: Categorical column + Numeric column(s) -> Bar chart primary
    if cat_cols and num_cols:
        c_col = cat_cols[0]
        n_col = num_cols[0]
        suggestions.append(
            PlotSuggestion(
                chart_type="bar",
                x_col=c_col.name,
                y_col=n_col.name,
                title=f"{n_col.name} by {c_col.name}",
                description=f"Bar chart of {n_col.name} grouped by {c_col.name}",
                confidence=0.95,
            )
        )
        suggestions.append(
            PlotSuggestion(
                chart_type="line",
                x_col=c_col.name,
                y_col=n_col.name,
                title=f"{n_col.name} by {c_col.name}",
                description=f"Line plot of {n_col.name} by {c_col.name}",
                confidence=0.75,
            )
        )
        suggestions.append(
            PlotSuggestion(
                chart_type="count",
                x_col=c_col.name,
                y_col=None,
                title=f"Distribution of {c_col.name}",
                description=f"Value counts of {c_col.name}",
                confidence=0.70,
            )
        )
        suggestions.append(
            PlotSuggestion(
                chart_type="hist",
                x_col=n_col.name,
                y_col=None,
                title=f"Histogram of {n_col.name}",
                description=f"Distribution histogram of {n_col.name}",
                confidence=0.65,
            )
        )
        if len(num_cols) >= 2:
            suggestions.append(
                PlotSuggestion(
                    chart_type="scatter",
                    x_col=num_cols[0].name,
                    y_col=num_cols[1].name,
                    title=f"{num_cols[1].name} vs {num_cols[0].name}",
                    description="Scatter plot between numeric columns",
                    confidence=0.80,
                )
            )

    # Case 4: Purely numeric (no categorical, no temporal)
    if num_cols and not cat_cols and not temp_cols:
        if len(num_cols) >= 2:
            suggestions.append(
                PlotSuggestion(
                    chart_type="scatter",
                    x_col=num_cols[0].name,
                    y_col=num_cols[1].name,
                    title=f"{num_cols[1].name} vs {num_cols[0].name}",
                    description=f"Scatter plot of {num_cols[1].name} vs {num_cols[0].name}",
                    confidence=0.95,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="line",
                    x_col=num_cols[0].name,
                    y_col=num_cols[1].name,
                    title=f"{num_cols[1].name} over {num_cols[0].name}",
                    description=f"Line plot of {num_cols[1].name} vs {num_cols[0].name}",
                    confidence=0.85,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="bar",
                    x_col=num_cols[0].name,
                    y_col=num_cols[1].name,
                    title=f"{num_cols[1].name} by {num_cols[0].name}",
                    description=f"Bar chart of {num_cols[1].name} by {num_cols[0].name}",
                    confidence=0.75,
                )
            )
            suggestions.append(
                PlotSuggestion(
                    chart_type="hist",
                    x_col=num_cols[0].name,
                    y_col=None,
                    title=f"Histogram of {num_cols[0].name}",
                    description=f"Distribution histogram of {num_cols[0].name}",
                    confidence=0.70,
                )
            )
        else:
            suggestions.append(
                PlotSuggestion(
                    chart_type="hist",
                    x_col=num_cols[0].name,
                    y_col=None,
                    title=f"Distribution of {num_cols[0].name}",
                    description=f"Histogram of {num_cols[0].name}",
                    confidence=0.95,
                )
            )

    # Case 5: Multiple categorical columns and no numeric
    if cat_cols and not num_cols and not temp_cols:
        suggestions.append(
            PlotSuggestion(
                chart_type="count",
                x_col=cat_cols[0].name,
                y_col=None,
                title=f"Count of {cat_cols[0].name}",
                description=f"Frequency counts for {cat_cols[0].name}",
                confidence=0.90,
            )
        )
        suggestions.append(
            PlotSuggestion(
                chart_type="bar",
                x_col=cat_cols[0].name,
                y_col=None,
                title=f"Bar chart of {cat_cols[0].name}",
                description="Bar chart of categories",
                confidence=0.60,
            )
        )

    # Fallback if no specific condition matched
    if not suggestions and profiles:
        default_col = profiles[0].name
        suggestions.append(
            PlotSuggestion(
                chart_type="bar",
                x_col=default_col,
                y_col=profiles[1].name if len(profiles) > 1 else None,
                title="Plot",
                description="Default bar chart",
                confidence=0.50,
            )
        )

    # Deduplicate while preserving order
    seen = set()
    unique_suggestions = []
    for s in sorted(suggestions, key=lambda x: x.confidence, reverse=True):
        key = (s.chart_type, s.x_col, s.y_col)
        if key not in seen:
            seen.add(key)
            unique_suggestions.append(s)

    return unique_suggestions


def detect_best_plot(
    column_names: Sequence[str], data: Sequence[Sequence[Any]]
) -> Optional[PlotSuggestion]:
    """Detect the single best recommended plot for the given dataset."""
    recs = recommend_plots(column_names, data)
    return recs[0] if recs else None


def render_plotext(
    column_names: Sequence[str],
    data: Sequence[Sequence[Any]],
    chart_type: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    y_cols: Optional[Sequence[str]] = None,
    title: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    theme: str = "dark",
) -> str:
    """Render a terminal chart using plotext.

    Args:
        column_names: List of column names in the dataset.
        data: Tabular rows.
        chart_type: One of ('bar', 'line', 'scatter', 'hist', 'count').
        x_col: Optional X column name.
        y_col: Optional single Y column name.
        y_cols: Optional list of Y column names.
        title: Optional chart title.
        width: Optional chart width in terminal cells.
        height: Optional chart height in terminal cells.
        theme: Theme name for plotext (default 'dark').

    Returns:
        ANSI string containing the rendered chart.
    """
    if not column_names or not data:
        return ""

    import plotext as plt

    # Consolidate y_cols
    resolved_y_cols: List[str] = []
    if y_cols:
        resolved_y_cols = [c for c in y_cols if c and c != "__none__" and c in column_names]
    elif y_col and y_col != "__none__" and y_col in column_names:
        resolved_y_cols = [y_col]

    resolved_x_col = x_col if x_col and x_col != "__none__" and x_col in column_names else None

    c_type = (chart_type or "bar").lower().strip()
    if c_type not in AVAILABLE_CHART_TYPES:
        c_type = "bar"

    col_map = {name: i for i, name in enumerate(column_names)}
    is_v5 = hasattr(plt, "clf")

    def _init_plot():
        if is_v5:
            plt.clf()
            try:
                plt.theme(theme)
            except Exception:
                pass
            if width and height:
                plt.plotsize(width, height)
            if title and title.strip():
                plt.title(title.strip())
        else:
            fig = plt.figure
            fig.clear()
            try:
                fig.theme(theme)
            except Exception:
                pass
            if width and height:
                fig.plot_size(width, height)
            if title and title.strip():
                fig.title(title.strip())

    _init_plot()

    if c_type == "count":
        target_col = resolved_x_col or column_names[0]
        col_idx = col_map[target_col]
        vals = [row[col_idx] for row in data if col_idx < len(row) and row[col_idx] is not None]
        if not vals:
            return "No data available to count."
        counter = Counter(vals)
        most_common = counter.most_common(30)
        x_labels = [str(k) for k, _ in most_common]
        y_counts = [int(v) for _, v in most_common]

        if is_v5:
            plt.bar(x_labels, y_counts)
            return plt.build()
        else:
            fig = plt.figure
            fig.draw(fig.bar(x_labels, y_counts))
            return fig.build()

    elif c_type == "hist":
        target_col = (
            resolved_y_cols[0]
            if resolved_y_cols
            else (resolved_x_col or column_names[0])
        )
        col_idx = col_map[target_col]
        num_vals: List[float] = []
        for row in data:
            if col_idx < len(row):
                val = row[col_idx]
                if is_numeric_value(val):
                    try:
                        num_vals.append(float(val))
                    except (ValueError, TypeError):
                        pass
        if not num_vals:
            return f"No numeric data in column '{target_col}' for histogram."

        if is_v5:
            plt.hist(num_vals)
            return plt.build()
        else:
            fig = plt.figure
            fig.draw(fig.hist(num_vals))
            return fig.build()

    elif c_type == "bar":
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        if resolved_y_cols:
            y_idx = col_map[resolved_y_cols[0]]
        else:
            y_idx = None
            for idx, col in enumerate(column_names):
                if idx != x_idx:
                    y_idx = idx
                    break
            if y_idx is None:
                y_idx = x_idx

        x_labels = []
        y_vals = []
        for row in data:
            if x_idx < len(row) and y_idx < len(row):
                y_raw = row[y_idx]
                if is_numeric_value(y_raw):
                    x_raw = row[x_idx]
                    x_labels.append(str(x_raw) if x_raw is not None else "")
                    y_vals.append(float(y_raw))

        if not y_vals:
            return "No numeric data available for bar chart."

        if len(x_labels) > 50:
            x_labels = x_labels[:50]
            y_vals = y_vals[:50]

        if is_v5:
            plt.bar(x_labels, y_vals)
            return plt.build()
        else:
            fig = plt.figure
            fig.draw(fig.bar(x_labels, y_vals))
            return fig.build()

    elif c_type == "scatter":
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        if resolved_y_cols:
            y_idx = col_map[resolved_y_cols[0]]
        else:
            y_idx = 1 if len(column_names) > 1 else 0

        x_points: List[float] = []
        y_points: List[float] = []
        for row in data:
            if x_idx < len(row) and y_idx < len(row):
                xv = row[x_idx]
                yv = row[y_idx]
                if is_numeric_value(xv) and is_numeric_value(yv):
                    x_points.append(float(xv))
                    y_points.append(float(yv))

        if not x_points:
            x_labels = []
            for idx, row in enumerate(data):
                if y_idx < len(row):
                    yv = row[y_idx]
                    if is_numeric_value(yv):
                        x_points.append(float(idx))
                        y_points.append(float(yv))
                        xv = row[x_idx] if x_idx < len(row) else ""
                        x_labels.append(str(xv) if xv is not None else "")

        if not y_points:
            return "No numeric data points available for scatter plot."

        if is_v5:
            plt.scatter(x_points, y_points)
            return plt.build()
        else:
            fig = plt.figure
            fig.draw(fig.signal(x_points, y_points, marker="dot").lines(False))
            return fig.build()

    elif c_type == "line":
        x_idx = col_map[resolved_x_col] if resolved_x_col else None
        target_y_cols = resolved_y_cols or ([column_names[1]] if len(column_names) > 1 else [column_names[0]])

        x_raw = []
        for row in data:
            if x_idx is not None and x_idx < len(row):
                x_raw.append(row[x_idx])
            else:
                x_raw.append(None)

        non_null_x = [x for x in x_raw if x is not None]
        all_x_num = bool(non_null_x and all(is_numeric_value(x) for x in non_null_x))

        if all_x_num:
            x_coords = [float(x) if is_numeric_value(x) else 0.0 for x in x_raw]
            has_ticks = False
            tick_labels: List[str] = []
        else:
            x_coords = list(range(len(data)))
            has_ticks = True
            tick_labels = [str(x) if x is not None else "" for x in x_raw]

        for y_name in target_y_cols:
            y_i = col_map.get(y_name)
            if y_i is None:
                continue
            y_series = []
            for row in data:
                val = row[y_i] if y_i < len(row) else 0
                y_series.append(float(val) if is_numeric_value(val) else 0.0)

            if is_v5:
                label = y_name if len(target_y_cols) > 1 else None
                plt.plot(x_coords, y_series, label=label)
            else:
                fig = plt.figure
                label = y_name if len(target_y_cols) > 1 else None
                sig = fig.signal(x_coords, y_series)
                if label:
                    sig.label(label)
                fig.draw(sig)

        if is_v5:
            if has_ticks and tick_labels:
                if len(tick_labels) > 10:
                    step = max(1, len(tick_labels) // 8)
                    indices = list(range(0, len(tick_labels), step))
                    labels = [tick_labels[i] for i in indices]
                    plt.xticks(indices, labels)
                else:
                    plt.xticks(x_coords, tick_labels)
            return plt.build()
        else:
            return plt.figure.build()

    return ""

