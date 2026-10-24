"""Plot detector heuristics and Plotly chart generation with Rich terminal formatting."""

from collections import Counter
import csv
from dataclasses import dataclass, field
import datetime
import decimal
import io
import os
import re
import sys
from typing import Any, List, Optional, Sequence, Tuple, Union

from PIL import Image
from rich.console import Console
from rich.style import Style
from rich.text import Text

AVAILABLE_CHART_TYPES = ("bar", "line", "scatter", "hist", "count", "pie", "box")

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


def create_plotly_figure(
    column_names: Sequence[str],
    data: Sequence[Sequence[Any]],
    chart_type: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    y_cols: Optional[Sequence[str]] = None,
    title: Optional[str] = None,
    theme: str = "plotly_dark",
):
    """Create a Plotly Figure object for the specified dataset and configuration.

    Args:
        column_names: List of column names in the dataset.
        data: Tabular rows.
        chart_type: One of ('bar', 'line', 'scatter', 'hist', 'count', 'pie', 'box').
        x_col: Optional X column name.
        y_col: Optional single Y column name.
        y_cols: Optional list of Y column names.
        title: Optional chart title.
        theme: Plotly template theme (default 'plotly_dark').

    Returns:
        plotly.graph_objects.Figure or error message string.
    """
    if not column_names or not data:
        return ""

    import plotly.graph_objects as go

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
    fig = go.Figure()

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
        fig.add_trace(go.Bar(x=x_labels, y=y_counts, name="Count", marker=dict(color="#00CC96")))

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
        fig.add_trace(go.Histogram(x=num_vals, name=target_col, nbinsx=30, marker=dict(color="#636EFA")))

    elif c_type == "bar":
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        if resolved_y_cols:
            target_y_cols = resolved_y_cols
        else:
            fallback_y = None
            for idx, col in enumerate(column_names):
                if idx != x_idx:
                    fallback_y = col
                    break
            target_y_cols = [fallback_y] if fallback_y else [column_names[x_idx]]

        has_any_numeric = False
        for y_name in target_y_cols:
            y_idx = col_map[y_name]
            x_labels = []
            y_vals = []
            for row in data:
                if x_idx < len(row) and y_idx < len(row):
                    y_raw = row[y_idx]
                    if is_numeric_value(y_raw):
                        x_raw = row[x_idx]
                        x_labels.append(str(x_raw) if x_raw is not None else "")
                        y_vals.append(float(y_raw))
                        has_any_numeric = True
            if y_vals:
                fig.add_trace(go.Bar(x=x_labels, y=y_vals, name=y_name))

        if not has_any_numeric:
            return "No numeric data available for bar chart."

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
            for idx, row in enumerate(data):
                if y_idx < len(row):
                    yv = row[y_idx]
                    if is_numeric_value(yv):
                        x_points.append(float(idx))
                        y_points.append(float(yv))

        if not y_points:
            return "No numeric data points available for scatter plot."

        fig.add_trace(
            go.Scatter(
                x=x_points,
                y=y_points,
                mode="markers",
                marker=dict(size=8, opacity=0.85, line=dict(width=1, color="white")),
                name="Points",
            )
        )

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
        else:
            x_coords = [str(x) if x is not None else "" for x in x_raw]

        for y_name in target_y_cols:
            y_i = col_map.get(y_name)
            if y_i is None:
                continue
            y_series = []
            for row in data:
                val = row[y_i] if y_i < len(row) else 0
                y_series.append(float(val) if is_numeric_value(val) else 0.0)

            fig.add_trace(
                go.Scatter(
                    x=x_coords,
                    y=y_series,
                    mode="lines+markers",
                    line=dict(width=3),
                    marker=dict(size=7),
                    name=y_name,
                )
            )

    elif c_type == "pie":
        target_col = resolved_x_col or column_names[0]
        col_idx = col_map[target_col]
        vals = [row[col_idx] for row in data if col_idx < len(row) and row[col_idx] is not None]
        if not vals:
            return "No data available to plot."
        counter = Counter(vals)
        most_common = counter.most_common(20)
        labels = [str(k) for k, _ in most_common]
        counts = [int(v) for _, v in most_common]
        fig.add_trace(go.Pie(labels=labels, values=counts, hole=0.3))

    elif c_type == "box":
        target_y_cols = resolved_y_cols or ([column_names[1]] if len(column_names) > 1 else [column_names[0]])
        for y_name in target_y_cols:
            y_i = col_map.get(y_name)
            if y_i is None:
                continue
            y_series = [float(row[y_i]) for row in data if y_i < len(row) and is_numeric_value(row[y_i])]
            fig.add_trace(go.Box(y=y_series, name=y_name))

    # Apply layout theme
    chart_title = title.strip() if title and title.strip() else f"Interactive {c_type.title()} Chart"
    fig.update_layout(
        title=dict(
            text=chart_title,
            x=0.5,
            font=dict(size=18, color="#ffffff"),
        ),
        template=theme,
        paper_bgcolor="#181825",
        plot_bgcolor="#1e1e2e",
        font=dict(color="#cdd6f4", family="sans-serif"),
        margin=dict(l=60, r=40, t=60, b=60),
        hovermode="closest",
        xaxis=dict(
            gridcolor="#313244",
            title=dict(text=resolved_x_col or "", font=dict(color="#a6adc8")),
        ),
        yaxis=dict(
            gridcolor="#313244",
            title=dict(text=", ".join(resolved_y_cols) if resolved_y_cols else "", font=dict(color="#a6adc8")),
        ),
    )

    return fig


BRAILLE_OFFSET = 0x2800
BRAILLE_DOTS = [
    [0x01, 0x08],
    [0x02, 0x10],
    [0x04, 0x20],
    [0x40, 0x80],
]


def fig_to_rich_image(
    fig,
    width: Optional[int] = None,
    height: Optional[int] = None,
    mode: str = "braille",
) -> Optional[Text]:
    """Convert a Plotly figure to a detailed terminal image using Rich Text and truecolor.

    Supported modes:
        - 'braille': Ultra-high detail (2x4 subpixels per cell, 8x pixel density).
        - 'halfblock': Smooth truecolor blocks (1x2 pixels per cell).
    """
    target_w = max(30, min(160, width if width else 90))
    target_h = max(10, min(50, height if height else 24))

    try:
        if mode == "braille":
            px_w = target_w * 2
            px_h = target_h * 4
        else:
            px_w = target_w
            px_h = target_h * 2

        png_bytes = fig.to_image(
            format="png",
            width=max(800, px_w * 4),
            height=max(500, px_h * 4),
        )

        im = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        im = im.resize((px_w, px_h), Image.Resampling.LANCZOS)
        pixels = im.load()

        text = Text()

        if mode == "braille":
            bg_r, bg_g, bg_b = pixels[0, 0]
            bg_lum = 0.299 * bg_r + 0.587 * bg_g + 0.114 * bg_b

            for cy in range(target_h):
                curr_style = None
                curr_chars = []
                for cx in range(target_w):
                    char_code = 0
                    r_sum, g_sum, b_sum = 0, 0, 0
                    dot_count = 0
                    for dy in range(4):
                        for dx in range(2):
                            r, g, b = pixels[cx * 2 + dx, cy * 4 + dy]
                            lum = 0.299 * r + 0.587 * g + 0.114 * b
                            diff = abs(lum - bg_lum)
                            color_diff = abs(r - bg_r) + abs(g - bg_g) + abs(b - bg_b)
                            if diff > 20 or color_diff > 35:
                                char_code |= BRAILLE_DOTS[dy][dx]
                                r_sum += r
                                g_sum += g
                                b_sum += b
                                dot_count += 1
                    ch = chr(BRAILLE_OFFSET + char_code) if char_code else " "
                    if dot_count > 0:
                        style = Style(
                            color=f"rgb({r_sum // dot_count},{g_sum // dot_count},{b_sum // dot_count})",
                            bgcolor=f"rgb({bg_r},{bg_g},{bg_b})",
                        )
                    else:
                        style = Style(bgcolor=f"rgb({bg_r},{bg_g},{bg_b})")

                    if style == curr_style:
                        curr_chars.append(ch)
                    else:
                        if curr_style is not None:
                            text.append("".join(curr_chars), style=curr_style)
                        curr_style = style
                        curr_chars = [ch]
                if curr_style is not None:
                    text.append("".join(curr_chars), style=curr_style)
                text.append("\n")

        else:  # Half-block mode
            for y in range(0, target_h * 2, 2):
                curr_style = None
                curr_chars = []
                for x in range(target_w):
                    r1, g1, b1 = pixels[x, y]
                    r2, g2, b2 = pixels[x, y + 1] if y + 1 < target_h * 2 else (0, 0, 0)
                    style = Style(color=f"rgb({r1},{g1},{b1})", bgcolor=f"rgb({r2},{g2},{b2})")
                    if style == curr_style:
                        curr_chars.append("▀")
                    else:
                        if curr_style is not None:
                            text.append("".join(curr_chars), style=curr_style)
                        curr_style = style
                        curr_chars = ["▀"]
                if curr_style is not None:
                    text.append("".join(curr_chars), style=curr_style)
                text.append("\n")

        return text
    except Exception:
        return None


def render_sharp_plot(
    column_names: Sequence[str],
    data: Sequence[Sequence[Any]],
    chart_type: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    y_cols: Optional[Sequence[str]] = None,
    title: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> Union[Text, str]:
    """Render a crystal-clear, razor-sharp vector plot using native terminal fonts and Rich."""
    c_type = (chart_type or "bar").lower().strip()
    col_map = {name: i for i, name in enumerate(column_names)}
    w = max(40, width if width else 80)
    h = max(14, height if height else 20)

    resolved_y_cols = []
    if y_cols:
        resolved_y_cols = [c for c in y_cols if c and c != "__none__" and c in column_names]
    elif y_col and y_col != "__none__" and y_col in column_names:
        resolved_y_cols = [y_col]
    resolved_x_col = x_col if x_col and x_col != "__none__" and x_col in column_names else None

    # Error checks matching existing behavior
    if c_type == "bar":
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
        y_has_num = any(y_idx < len(row) and is_numeric_value(row[y_idx]) for row in data)
        if not y_has_num:
            return "No numeric data available for bar chart."

    elif c_type == "hist":
        target_col = resolved_y_cols[0] if resolved_y_cols else (resolved_x_col or column_names[0])
        col_idx = col_map[target_col]
        has_num = any(col_idx < len(row) and is_numeric_value(row[col_idx]) for row in data)
        if not has_num:
            return f"No numeric data in column '{target_col}' for histogram."

    elif c_type == "scatter":
        if resolved_y_cols:
            y_idx = col_map[resolved_y_cols[0]]
        else:
            y_idx = 1 if len(column_names) > 1 else 0
        has_num = any(y_idx < len(row) and is_numeric_value(row[y_idx]) for row in data)
        if not has_num:
            return "No numeric data points available for scatter plot."

    t = Text()
    badge_style = Style(color="black", bgcolor="bright_cyan", bold=True)
    type_badge_style = Style(color="bright_white", bgcolor="blue", bold=True)
    title_style = Style(color="bright_cyan", bold=True)

    chart_title = title.strip() if title and title.strip() else f"{c_type.title()} Chart"
    t.append(" 📊 PLOTLY ", style=badge_style)
    t.append(" ")
    t.append(f" {c_type.upper()} ", style=type_badge_style)
    t.append(f"  {chart_title}\n\n", style=title_style)

    BLOCKS = [" ", " ", "▂", "▃", "▄", "▅", "▆", "▇", "█"]

    if c_type in ("bar", "count"):
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        if c_type == "count":
            vals = [row[x_idx] for row in data if x_idx < len(row) and row[x_idx] is not None]
            if not vals:
                return "No data available to count."
            counter = Counter(vals)
            items = counter.most_common(20)
            labels = [str(k) for k, _ in items]
            values = [float(v) for _, v in items]
        else:
            y_idx = col_map[resolved_y_cols[0]] if resolved_y_cols else (1 if len(column_names) > 1 else 0)
            items = []
            for row in data:
                if x_idx < len(row) and y_idx < len(row):
                    y_raw = row[y_idx]
                    if is_numeric_value(y_raw):
                        items.append((str(row[x_idx]), float(y_raw)))
            if not items:
                return "No numeric data available for bar chart."
            labels = [k for k, _ in items[:25]]
            values = [v for _, v in items[:25]]

        n = len(values)
        max_val = max(values, default=1.0)
        min_val = min(0.0, min(values, default=0.0))
        val_range = max(1e-9, max_val - min_val)

        if n <= 14:
            y_axis_w = 8
            avail_w = max(20, w - y_axis_w - 4)
            col_w = max(5, min(14, avail_w // max(1, n)))
            bar_w = max(2, col_w - 2)
            plot_h = max(6, min(24, h - 6))

            grid = [[" " * bar_w for _ in range(n)] for _ in range(plot_h)]
            for i, v in enumerate(values):
                tot_units = int(((v - min_val) / val_range) * plot_h * 8)
                for r in range(plot_h):
                    row_idx = plot_h - 1 - r
                    row_units = max(0, min(8, tot_units - (r * 8)))
                    grid[row_idx][i] = BLOCKS[row_units] * bar_w

            for r in range(plot_h):
                y_val = min_val + ((plot_h - 1 - r) / max(1, plot_h - 1)) * val_range
                t.append(f"{y_val:>6.1f} ┤ ", style=Style(color="bright_black"))
                for i in range(n):
                    pad_l = (col_w - bar_w) // 2
                    pad_r = col_w - bar_w - pad_l
                    t.append(" " * pad_l)
                    t.append(grid[r][i], style=Style(color="bright_cyan", bold=True))
                    t.append(" " * pad_r)
                t.append("\n")

            t.append(" " * 7 + "┼" + "─" * (n * col_w + 2) + "\n", style=Style(color="bright_black"))
            t.append(" " * 9)
            for lab in labels:
                t.append(str(lab)[:col_w].center(col_w), style=Style(color="bright_white", bold=True))
            t.append("\n\n")
        else:
            max_label_w = min(18, max((len(str(l)) for l in labels), default=6))
            val_w = 10
            bar_max_w = max(15, w - max_label_w - val_w - 14)
            tot_sum = sum(values) if sum(values) > 0 else 1.0
            for lab, val in zip(labels, values):
                ratio = (val - min_val) / val_range if val_range > 0 else 0
                bar_len = int(ratio * bar_max_w)
                pct = (val / tot_sum) * 100
                t.append(f"  {str(lab)[:max_label_w]:<{max_label_w}} ", style=Style(color="bright_white", bold=True))
                t.append(f"│ {val:>8.2f} ", style=Style(color="bright_green"))
                t.append("█" * bar_len, style=Style(color="bright_cyan", bold=True))
                t.append(f" ({pct:.1f}%)\n", style=Style(color="bright_black"))
            t.append("\n")

    elif c_type in ("line", "scatter"):
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        target_y_cols = resolved_y_cols or ([column_names[1]] if len(column_names) > 1 else [column_names[0]])
        x_labels = [str(row[x_idx]) if x_idx < len(row) and row[x_idx] is not None else "" for row in data]

        series_dict = {}
        for y_name in target_y_cols:
            y_i = col_map.get(y_name)
            if y_i is None:
                continue
            y_vals = []
            for row in data:
                val = row[y_i] if y_i < len(row) else 0
                y_vals.append(float(val) if is_numeric_value(val) else 0.0)
            if y_vals:
                series_dict[y_name] = y_vals

        COLORS = ["bright_cyan", "bright_magenta", "bright_yellow", "bright_green"]
        if len(series_dict) > 1:
            t.append("  Legend: ")
            for i, (name, _) in enumerate(series_dict.items()):
                col = COLORS[i % len(COLORS)]
                mark = "●" if c_type == "scatter" else "──"
                t.append(f"{mark} {name}   ", style=Style(color=col, bold=True))
            t.append("\n\n")

        all_y = [v for s in series_dict.values() for v in s]
        if not all_y:
            return "No numeric data points available."
        min_y = min(all_y)
        max_y = max(all_y)
        if min_y == max_y:
            min_y -= 1.0
            max_y += 1.0
        range_y = max_y - min_y

        y_axis_w = 8
        canvas_w = max(20, w - y_axis_w - 4)
        canvas_h = max(6, min(24, h - 6))

        px_w = canvas_w * 2
        px_h = canvas_h * 4

        DOTS = [[0x01, 0x08], [0x02, 0x10], [0x04, 0x20], [0x40, 0x80]]
        grid = [[0 for _ in range(canvas_w)] for _ in range(canvas_h)]
        color_grid = [[None for _ in range(canvas_w)] for _ in range(canvas_h)]

        def set_dot(x, y, color):
            cx = max(0, min(canvas_w - 1, x // 2))
            cy = max(0, min(canvas_h - 1, canvas_h - 1 - (y // 4)))
            dx = x % 2
            dy = 3 - (y % 4)
            grid[cy][cx] |= DOTS[dy][dx]
            if color_grid[cy][cx] is None:
                color_grid[cy][cx] = color

        for s_idx, (s_name, y_vals) in enumerate(series_dict.items()):
            col_name = COLORS[s_idx % len(COLORS)]
            n = len(y_vals)
            pts = []
            for i, y in enumerate(y_vals):
                px = int((i / max(1, n - 1)) * (px_w - 1)) if n > 1 else px_w // 2
                py = int(((y - min_y) / range_y) * (px_h - 1))
                py = max(0, min(px_h - 1, py))
                pts.append((px, py))

            if c_type == "scatter":
                for px, py in pts:
                    set_dot(px, py, col_name)
                    if px + 1 < px_w:
                        set_dot(px + 1, py, col_name)
                    if py + 1 < px_h:
                        set_dot(px, py + 1, col_name)
            else:
                for i in range(len(pts) - 1):
                    x0, y0 = pts[i]
                    x1, y1 = pts[i + 1]
                    dist = max(abs(x1 - x0), abs(y1 - y0), 1)
                    for step in range(dist + 1):
                        ix = int(x0 + (x1 - x0) * (step / dist))
                        iy = int(y0 + (y1 - y0) * (step / dist))
                        set_dot(ix, iy, col_name)

        for r in range(canvas_h):
            y_val = max_y - (r / max(1, canvas_h - 1)) * range_y
            t.append(f"{y_val:>6.1f} ┤ ", style=Style(color="bright_black"))
            curr_color = None
            curr_chars = []
            for c in range(canvas_w):
                code = grid[r][c]
                ch = chr(0x2800 + code) if code else " "
                c_color = color_grid[r][c] or "bright_cyan"
                if c_color == curr_color:
                    curr_chars.append(ch)
                else:
                    if curr_chars:
                        t.append("".join(curr_chars), style=Style(color=curr_color or "bright_cyan", bold=True))
                    curr_color = c_color
                    curr_chars = [ch]
            if curr_chars:
                t.append("".join(curr_chars), style=Style(color=curr_color or "bright_cyan", bold=True))
            t.append("\n")

        t.append(" " * 7 + "┼" + "─" * (canvas_w + 2) + "\n", style=Style(color="bright_black"))
        n_labels = len(x_labels)
        if n_labels > 0:
            label_line = [" " for _ in range(canvas_w)]
            step = max(1, n_labels // 5)
            for i in range(0, n_labels, step):
                lbl = str(x_labels[i])
                pos = int((i / max(1, n_labels - 1)) * (canvas_w - len(lbl))) if n_labels > 1 else 0
                for ci, ch in enumerate(lbl):
                    if pos + ci < canvas_w:
                        label_line[pos + ci] = ch
            t.append(" " * 9 + "".join(label_line) + "\n\n", style=Style(color="bright_white", bold=True))

    elif c_type == "hist":
        target_col = resolved_y_cols[0] if resolved_y_cols else (resolved_x_col or column_names[0])
        col_idx = col_map[target_col]
        nums = [float(row[col_idx]) for row in data if col_idx < len(row) and is_numeric_value(row[col_idx])]
        if not nums:
            return f"No numeric data in column '{target_col}' for histogram."

        num_bins = min(12, max(5, len(nums) // 3))
        min_v = min(nums)
        max_v = max(nums)
        if min_v == max_v:
            max_v += 1.0
        span = max_v - min_v
        bin_w = span / num_bins
        bins = [0] * num_bins
        for val in nums:
            idx = min(num_bins - 1, int((val - min_v) / bin_w))
            bins[idx] += 1

        bin_labels = [f"{min_v + i*bin_w:.0f}" for i in range(num_bins)]
        max_count = max(bins)
        plot_h = max(6, min(20, h - 6))
        col_w = max(5, min(12, (w - 12) // num_bins))
        bar_w = max(2, col_w - 2)

        for r in range(plot_h):
            row_y = max_count * (plot_h - r) / plot_h
            t.append(f"{row_y:>5.0f} ┤ ", style=Style(color="bright_black"))
            for b_idx in range(num_bins):
                pad_l = (col_w - bar_w) // 2
                pad_r = col_w - bar_w - pad_l
                t.append(" " * pad_l)
                bar_ratio = bins[b_idx] / max_count if max_count > 0 else 0
                cell_ratio = (plot_h - r) / plot_h
                prev_ratio = (plot_h - r - 1) / plot_h
                if bar_ratio >= cell_ratio:
                    t.append("█" * bar_w, style=Style(color="bright_cyan", bold=True))
                elif bar_ratio > prev_ratio:
                    step_idx = int(((bar_ratio - prev_ratio) / (cell_ratio - prev_ratio)) * 8)
                    t.append(BLOCKS[max(1, min(8, step_idx))] * bar_w, style=Style(color="bright_cyan", bold=True))
                else:
                    t.append(" " * bar_w)
                t.append(" " * pad_r)
            t.append("\n")

        t.append(" " * 6 + "┼" + "─" * (num_bins * col_w + 2) + "\n", style=Style(color="bright_black"))
        t.append(" " * 8)
        for lab in bin_labels:
            t.append(lab.center(col_w), style=Style(color="bright_white", bold=True))
        t.append("\n\n")

    elif c_type == "box":
        target_y_cols = resolved_y_cols or ([column_names[1]] if len(column_names) > 1 else [column_names[0]])
        for y_name in target_y_cols:
            y_i = col_map.get(y_name)
            if y_i is None:
                continue
            vals = [float(row[y_i]) for row in data if y_i < len(row) and is_numeric_value(row[y_i])]
            if not vals:
                continue
            vals.sort()
            min_v = vals[0]
            max_v = vals[-1]
            q1 = vals[len(vals) // 4]
            med = vals[len(vals) // 2]
            q3 = vals[(len(vals) * 3) // 4]
            span = max(1e-9, max_v - min_v)
            box_w = max(30, min(80, w - 16))

            def get_pos(v):
                return int(((v - min_v) / span) * (box_w - 1))

            p_min, p_q1, p_med, p_q3, p_max = get_pos(min_v), get_pos(q1), get_pos(med), get_pos(q3), get_pos(max_v)
            line = [" " for _ in range(box_w)]
            for i in range(p_min, p_q1):
                line[i] = "─"
            for i in range(p_q3 + 1, p_max + 1):
                line[i] = "─"
            line[p_min] = "├"
            line[p_max] = "┤"
            for i in range(p_q1, p_q3 + 1):
                line[i] = "█"
            line[p_med] = "┃"

            t.append(f"  {y_name}:\n", style=Style(color="bright_white", bold=True))
            t.append(f"  Min: {min_v:.2f}  Q1: {q1:.2f}  Median: {med:.2f}  Q3: {q3:.2f}  Max: {max_v:.2f}\n", style=Style(color="bright_black"))
            t.append("  " + "".join(line) + "\n", style=Style(color="bright_cyan", bold=True))
            t.append(f"  {min_v:<10.1f}{med:^{box_w-20}.1f}{max_v:>10.1f}\n\n", style=Style(color="bright_black"))

    elif c_type == "pie":
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        vals = [row[x_idx] for row in data if x_idx < len(row) and row[x_idx] is not None]
        counter = Counter(vals)
        top = counter.most_common(10)
        tot = sum(c for _, c in top) or 1
        COLORS = ["bright_cyan", "bright_magenta", "bright_yellow", "bright_green", "bright_blue", "bright_red"]
        bar_len = max(30, min(80, w - 10))

        t.append("  [", style=Style(color="bright_black"))
        for i, (k, c) in enumerate(top):
            seg_len = max(1, int((c / tot) * bar_len))
            t.append("█" * seg_len, style=Style(color=COLORS[i % len(COLORS)], bold=True))
        t.append("]\n\n", style=Style(color="bright_black"))

        for i, (k, c) in enumerate(top):
            pct = (c / tot) * 100
            t.append(f"  ■ ", style=Style(color=COLORS[i % len(COLORS)], bold=True))
            t.append(f"{str(k)[:20]:<20} ", style=Style(color="bright_white"))
            t.append(f"{c:>6} ({pct:>5.1f}%)\n", style=Style(color="bright_black"))
        t.append("\n")

    return t


def render_plotly(
    column_names: Sequence[str],
    data: Sequence[Sequence[Any]],
    chart_type: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    y_cols: Optional[Sequence[str]] = None,
    title: Optional[str] = None,
    width: Optional[int] = None,
    height: Optional[int] = None,
    theme: str = "plotly_dark",
    mode: str = "vector",
    as_str: bool = False,
) -> Union[Text, str]:
    """Render a Plotly chart in the terminal using Rich.

    Modes:
        - 'vector' (default): Ultra-sharp vector chart with native text axes and subpixels.
        - 'braille': Rasterized Braille image.
        - 'halfblock': Rasterized TrueColor half-blocks.
    """
    if not column_names or not data:
        return "" if as_str else Text("No query data available to plot.", style=Style(color="yellow"))

    # Vector mode: native crisp vector text & axes
    if mode == "vector":
        out = render_sharp_plot(
            column_names=column_names,
            data=data,
            chart_type=chart_type,
            x_col=x_col,
            y_col=y_col,
            y_cols=y_cols,
            title=title,
            width=width,
            height=height,
        )
        if isinstance(out, str):
            return out if as_str else Text(out, style=Style(color="bright_yellow", bold=True))
        return out.plain if as_str else out

    # Raster mode: converts Plotly PNG to half-block / braille image
    fig_or_err = create_plotly_figure(
        column_names=column_names,
        data=data,
        chart_type=chart_type,
        x_col=x_col,
        y_col=y_col,
        y_cols=y_cols,
        title=title,
        theme=theme,
    )

    if isinstance(fig_or_err, str):
        return fig_or_err if as_str else Text(fig_or_err, style=Style(color="bright_yellow", bold=True))

    c_type = (chart_type or "bar").lower().strip()
    badge_style = Style(color="black", bgcolor="bright_cyan", bold=True)
    type_badge_style = Style(color="bright_white", bgcolor="blue", bold=True)
    title_style = Style(color="bright_cyan", bold=True)

    result = Text()
    result.append(" 📊 PLOTLY ", style=badge_style)
    result.append(" ")
    result.append(f" {c_type.upper()} ", style=type_badge_style)
    result.append("  ")
    result.append(title.strip() if title and title.strip() else f"{c_type.title()} Chart", style=title_style)
    result.append("\n\n")

    img_text = fig_to_rich_image(fig_or_err, width=width, height=height, mode=mode)
    if img_text:
        result.append_text(img_text)
    else:
        fallback_text = render_plot_rich(
            column_names=column_names,
            data=data,
            chart_type=chart_type,
            x_col=x_col,
            y_col=y_col,
            y_cols=y_cols,
            title=title,
            width=width,
        )
        if isinstance(fallback_text, Text):
            result.append_text(fallback_text)

    if as_str:
        return result.plain

    return result


def render_plot_rich(
    column_names: Sequence[str],
    data: Sequence[Sequence[Any]],
    chart_type: str,
    x_col: Optional[str] = None,
    y_col: Optional[str] = None,
    y_cols: Optional[Sequence[str]] = None,
    title: Optional[str] = None,
    html_path: Optional[str] = None,
    width: Optional[int] = 80,
    as_str: bool = False,
) -> Union[Text, str]:
    """Render a rich, styled terminal summary card for the Plotly chart.

    Uses rich.console.Console, rich.text.Text, and rich.style.Style.
    """
    if not column_names or not data:
        return "" if as_str else Text("No query data available to plot.", style=Style(color="yellow"))

    console = Console(width=width or 80, record=True)

    title_style = Style(color="bright_cyan", bold=True)
    badge_style = Style(color="black", bgcolor="bright_cyan", bold=True)
    type_badge_style = Style(color="bright_white", bgcolor="blue", bold=True)
    label_style = Style(color="bright_blue", bold=True)
    val_style = Style(color="bright_white")
    dim_style = Style(color="bright_black")
    metric_style = Style(color="bright_green", bold=True)
    bar_fill_style = Style(color="bright_cyan")
    warn_style = Style(color="bright_yellow", bold=True)

    c_type = (chart_type or "bar").lower().strip()
    col_map = {name: i for i, name in enumerate(column_names)}

    resolved_y_cols = []
    if y_cols:
        resolved_y_cols = [c for c in y_cols if c and c != "__none__" and c in column_names]
    elif y_col and y_col != "__none__" and y_col in column_names:
        resolved_y_cols = [y_col]

    resolved_x_col = x_col if x_col and x_col != "__none__" and x_col in column_names else None

    if c_type == "bar":
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
        y_has_num = any(
            y_idx < len(row) and is_numeric_value(row[y_idx])
            for row in data
        )
        if not y_has_num:
            msg = "No numeric data available for bar chart."
            return msg if as_str else Text(msg, style=warn_style)

    elif c_type == "hist":
        target_col = resolved_y_cols[0] if resolved_y_cols else (resolved_x_col or column_names[0])
        col_idx = col_map[target_col]
        has_num = any(col_idx < len(row) and is_numeric_value(row[col_idx]) for row in data)
        if not has_num:
            msg = f"No numeric data in column '{target_col}' for histogram."
            return msg if as_str else Text(msg, style=warn_style)

    elif c_type == "scatter":
        if resolved_y_cols:
            y_idx = col_map[resolved_y_cols[0]]
        else:
            y_idx = 1 if len(column_names) > 1 else 0
        has_num = any(y_idx < len(row) and is_numeric_value(row[y_idx]) for row in data)
        if not has_num:
            msg = "No numeric data points available for scatter plot."
            return msg if as_str else Text(msg, style=warn_style)

    rich_text = Text()
    rich_text.append(" 📊 PLOTLY ", style=badge_style)
    rich_text.append(" ")
    rich_text.append(f" {c_type.upper()} ", style=type_badge_style)
    rich_text.append("  ")
    rich_text.append(title if title else f"Interactive {c_type.title()} Chart", style=title_style)
    rich_text.append("\n\n")

    rich_text.append("• Chart Type: ", style=label_style)
    rich_text.append(f"{c_type.title()}\n", style=val_style)
    rich_text.append("• X / Category: ", style=label_style)
    rich_text.append(f"{resolved_x_col or '(Index)'}\n", style=val_style)
    rich_text.append("• Y / Value(s): ", style=label_style)
    rich_text.append(f"{', '.join(resolved_y_cols) if resolved_y_cols else '(Frequencies)'}\n", style=val_style)
    rich_text.append("• Total Rows: ", style=label_style)
    rich_text.append(f"{len(data):,} records\n\n", style=val_style)

    rich_text.append("── Data Summary Preview ──────────────────────────────────────\n", style=dim_style)
    if c_type in ("bar", "count"):
        x_idx = col_map[resolved_x_col] if resolved_x_col else 0
        if c_type == "count":
            vals = [row[x_idx] for row in data if x_idx < len(row) and row[x_idx] is not None]
            counter = Counter(vals)
            top_items = counter.most_common(6)
            max_v = top_items[0][1] if top_items else 1
            for k, count in top_items:
                bar_len = int((count / max_v) * 25)
                bar_str = "■" * max(1, bar_len)
                rich_text.append(f"  {str(k)[:18]:<18} ", style=val_style)
                rich_text.append(f"{count:>6} ", style=metric_style)
                rich_text.append(f"{bar_str}\n", style=bar_fill_style)
        else:
            y_idx = col_map[resolved_y_cols[0]] if resolved_y_cols else (1 if len(column_names) > 1 else 0)
            items = []
            for row in data:
                if x_idx < len(row) and y_idx < len(row):
                    y_raw = row[y_idx]
                    if is_numeric_value(y_raw):
                        items.append((str(row[x_idx]), float(y_raw)))
            top_items = items[:6]
            max_v = max([v for _, v in top_items], default=1.0)
            for k, val in top_items:
                ratio = (val / max_v) if max_v > 0 else 0
                bar_len = int(ratio * 25)
                bar_str = "■" * max(1, bar_len)
                rich_text.append(f"  {str(k)[:18]:<18} ", style=val_style)
                rich_text.append(f"{val:>8.2f} ", style=metric_style)
                rich_text.append(f"{bar_str}\n", style=bar_fill_style)

    elif c_type in ("line", "scatter", "hist"):
        target_y = resolved_y_cols[0] if resolved_y_cols else (resolved_x_col or column_names[0])
        col_idx = col_map.get(target_y, 0)
        nums = []
        for row in data:
            if col_idx < len(row):
                v = row[col_idx]
                if is_numeric_value(v):
                    nums.append(float(v))
        if nums:
            min_v = min(nums)
            max_v = max(nums)
            avg_v = sum(nums) / len(nums)
            rich_text.append(f"  Column '{target_y}' Metrics:\n", style=label_style)
            rich_text.append("  • Count: ", style=dim_style)
            rich_text.append(f"{len(nums):,}  ", style=metric_style)
            rich_text.append("• Min: ", style=dim_style)
            rich_text.append(f"{min_v:.2f}  ", style=metric_style)
            rich_text.append("• Max: ", style=dim_style)
            rich_text.append(f"{max_v:.2f}  ", style=metric_style)
            rich_text.append("• Mean: ", style=dim_style)
            rich_text.append(f"{avg_v:.2f}\n", style=metric_style)

    rich_text.append("──────────────────────────────────────────────────────────────\n\n", style=dim_style)

    if as_str:
        return rich_text.plain

    return rich_text


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
    """Compatibility wrapper returning string-rendered terminal plot."""
    return render_plotly(
        column_names=column_names,
        data=data,
        chart_type=chart_type,
        x_col=x_col,
        y_col=y_col,
        y_cols=y_cols,
        title=title,
        width=width,
        height=height,
        as_str=True,
    )



