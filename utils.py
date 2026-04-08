import duckdb
import pathlib
import calendar
import datetime
import pandas as pd
from typing import List
from pathlib import Path
from collections import Counter

from pyecharts.charts import HeatMap
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode
from pyecharts.render.display import HTML

days = list(calendar.day_abbr)
months = list(calendar.month_abbr)[1:]


def list_options(data_path: str) -> dict[str, list[str]]:
    """
    List available well names and their end types from parquet files in the given directory.

    Args:
        data_path: Path to the directory containing parquet files.
                   Files are expected to follow the naming pattern: {well_name}_*_{end_type}_*.parquet

    Returns:
        Dictionary mapping well names to lists of available end types.
        Example: {'DELGT01': ['SE', 'DE'], 'DELGT02S2': ['SE', 'DE']}
    """
    files = list(pathlib.Path(data_path).glob("*.parquet"))
    options = dict()
    for f in files:
        parts = f.stem.split("_")
        well_name = parts[0]
        end_type = parts[2]
        if well_name not in options:
            options[well_name] = []
        if end_type not in options[well_name]:
            options[well_name].append(end_type)
    return options


def list_files(data_path: str, wellname: str, end_type: str) -> List[Path]:
    """
    List parquet files for a specific well and end type.

    Args:
        data_path: Path to the directory containing parquet files.
        wellname: Name of the well (e.g., 'DELGT01', 'DELGT02S2').
        end_type: Type of end measurement (e.g., 'SE', 'DE').

    Returns:
        Sorted list of Path objects matching the specified well and end type.

    Raises:
        ValueError: If wellname is not found in available options.
        ValueError: If end_type is not available for the specified well.
    """
    opts = list_options(data_path)
    if wellname not in opts:
        raise ValueError(f"Wellname not found. Possible options: {list(opts.keys())}")
    if end_type not in opts[wellname]:
        raise ValueError(
            f"End type not found. Possible options for {wellname}: {opts[wellname]}"
        )

    files = sorted(
        list(pathlib.Path(data_path).glob(f"{wellname}*_{end_type}_*.parquet"))
    )
    return files


def list_dates(data_path: str, wellname: str, end_type: str) -> list[datetime.datetime]:
    """
    List all measurement dates for a specific well and end type.

    Args:
        data_path: Path to the directory containing parquet files.
        wellname: Name of the well (e.g., 'DELGT01', 'DELGT02S2').
        end_type: Type of end measurement (e.g., 'SE', 'DE').

    Returns:
        List of datetime objects representing measurement timestamps,
        extracted from the parquet filenames.
    """
    files = list_files(data_path, wellname, end_type)
    dates = [
        datetime.datetime.strptime(i.stem.split("_")[4], "%Y%m%dT%H%M%S") for i in files
    ]
    return dates


def aggregate_date_occurrences(data: list[datetime.datetime]) -> list[list]:
    """
    Aggregate datetime objects by date and count occurrences.

    Args:
        data: List of datetime objects to aggregate.

    Returns:
        List of [date_string, count] pairs, where date_string is in 'YYYY-MM-DD' format
        and count is the number of occurrences on that date.
        Example: [['2025-12-11', 2], ['2025-12-12', 4]]
    """
    date_counter = Counter()
    formatted_data = [dt.strftime("%Y-%m-%d") for dt in data]
    for date in formatted_data:
        date_counter[date] += 1

    aggregated_data = [[date, count] for date, count in date_counter.items()]
    return aggregated_data


def plot_calendar(data, title, upper_range, save_html=False) -> HTML:
    """
    Plot a calendar heatmap showing data occurrences across 2025 and 2026.

    Args:
        data: List of [date_string, count] pairs where date_string is in 'YYYY-MM-DD' format.
              Example: [['2025-12-11', 2], ['2026-01-20', 4]]
        title: Title to display at the top of the calendar chart.
        upper_range: Maximum value for the visual map color scale.
        save_html: Whether to save the chart as an HTML file.

    Returns:
        Rendered notebook visualization of the calendar heatmap.
    """
    from pyecharts.charts import Calendar
    from pyecharts import options as opts

    data_2025 = [[i, j] for [i, j] in data if i.split("-")[0] == "2025"]
    data_2026 = [[i, j] for [i, j] in data if i.split("-")[0] == "2026"]
    calendar = (
        Calendar(init_opts=opts.InitOpts(height="480px"))
        .add(
            series_name="2025",
            yaxis_data=data_2025,
            calendar_opts=opts.CalendarOpts(
                range_="2025",
                pos_top="50",
                pos_left="60",
                pos_right="60",
                yearlabel_opts=opts.CalendarYearLabelOpts(is_show=True, margin=40),
                daylabel_opts=opts.CalendarDayLabelOpts(name_map=days),
                monthlabel_opts=opts.CalendarMonthLabelOpts(name_map=months),
            ),
        )
        .add(
            series_name="2026",
            yaxis_data=data_2026,
            calendar_index=1,
            calendar_opts=opts.CalendarOpts(
                range_="2026",
                pos_top="250",
                pos_left="60",
                pos_right="60",
                yearlabel_opts=opts.CalendarYearLabelOpts(is_show=True, margin=40),
                daylabel_opts=opts.CalendarDayLabelOpts(name_map=days),
                monthlabel_opts=opts.CalendarMonthLabelOpts(name_map=months),
            ),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(pos_top="0", pos_left="center", title=title),
            visualmap_opts=opts.VisualMapOpts(
                max_=upper_range,
                min_=0,
                orient="horizontal",
                pos_right="center",
                pos_bottom="0",
            ),
            legend_opts=opts.LegendOpts(is_show=False),
        )
    )
    if save_html:
        calendar.render(f"{'_'.join(title.split())}_calendar.html")
    return calendar.render_notebook()


# this function runs slower over the network access for the dts files
# you can replace it with the load_dts_data if you dont want to use duckdb

# def load_dts_temperature_data(files: List[Path], laf_column: str = "LAF", tmp_column: str = "TMP") -> pd.DataFrame:
#     """
#     Load TMP (temperature) data from parquet files into a DataFrame with LAF as index and dates as columns.

#     Args:
#         files: List of parquet file paths. Filenames should follow the pattern:
#                {wellname}_{...}_{end_type}_{datetime}_*.parquet where datetime is in %Y%m%dT%H%M%S format.
#         laf_column: Name of the LAF (Length Along Fiber) column in parquet files. Defaults to "LAF".
#         tmp_column: Name of the TMP (Temperature) column in parquet files. Defaults to "TMP".

#     Returns:
#         DataFrame with LAF values as index and datetime columns containing TMP values.
#         Each column represents a measurement timestamp extracted from the filename.

#     Example:
#         >>> files = list_files(FILE_PATH, "DELGT02S2", "SE")
#         >>> df = load_dts_temperature_data(files)
#         >>> df.head()
#     """
#     dfs = []

#     for f in files:
#         # Extract datetime from filename
#         parts = f.stem.split("_")
#         date_str = parts[3]
#         dt = datetime.datetime.strptime(date_str, "%Y%m%dT%H%M%S")

#         # Read parquet file
#         df = pd.read_parquet(f, columns=[laf_column, tmp_column])
#         df = df.set_index(laf_column)
#         df = df.rename(columns={tmp_column: dt})
#         dfs.append(df)

#     # Concatenate all dataframes, aligning by LAF index
#     result_df = pd.concat(dfs, axis=1)
#     result_df.index.name = laf_column
#     return result_df


def load_dts_data(
    files: List[Path], laf: str = "LAF", tmp: str = "TMP"
) -> pd.DataFrame:
    """
    Load TMP (temperature) data from parquet files into a DataFrame with LAF as index and dates as columns.

    Args:
        files: List of parquet file paths. Filenames should follow the pattern:
               {wellname}_{...}_{end_type}_{datetime}_*.parquet where datetime is in %Y%m%dT%H%M%S format.
        laf_column: Name of the LAF (Length Along Fiber) column in parquet files. Defaults to "LAF".
        tmp_column: Name of the TMP (Temperature) column in parquet files. Defaults to "TMP".

    Returns:
        DataFrame with LAF values as index and datetime columns containing TMP values.
        Each column represents a measurement timestamp extracted from the filename.

    Example:
        >>> files = list_files(FILE_PATH, "DELGT02S2", "SE")
        >>> df = load_dts_data(files)
        >>> df.head()
    """

    file_list = [str(f) for f in files]
    ts_regex: str = r"_(\d{8}T\d{6})_"
    ts_format: str = "%Y%m%dT%H%M%S"

    con = duckdb.connect(":memory:")

    con.execute("SET VARIABLE files = ?", [file_list])

    con.execute(
        f"""
        CREATE TEMP VIEW dts_base AS
        SELECT 
            {laf} AS laf, {tmp} AS tmp, 
            strptime(regexp_extract(filename, '{ts_regex}',1), '{ts_format}') AS ts
        from parquet_scan(getvariable('files'), filename=true, union_by_name=true)
        where regexp_extract(filename,'{ts_regex}',1) <> ''
    """
    )
    ts_list = [
        r[0]
        for r in con.execute("SELECT DISTINCT ts FROM dts_base ORDER BY ts").fetchall()
    ]

    pivot_in = ", ".join(
        f"TIMESTAMP '{ts.strftime('%Y-%m-%d %H:%M:%S')}'" for ts in ts_list
    )

    query = f"""
    select * 
    from (select laf, ts, tmp from dts_base)
    pivot (max(tmp) for ts in ({pivot_in}))
    order by laf
    """
    rel = con.sql(query)
    df = rel.df()
    df = df.set_index(["laf"])
    return df


def bin_dataframe(dataframe: pd.DataFrame, bin_size: int = 1) -> pd.DataFrame:
    """
    Bin LAF values and calculate average TMP values for each bin.

    Args:
        df: DataFrame with LAF as index
        bin_size: Size of each LAF bin (e.g., 1, 5, 10, 20)

    Returns:
        DataFrame with binned LAF values and averaged TMP
    """
    # Filter out LAF values below 0
    dataframe = dataframe[dataframe.index >= 0].copy()
    dataframe = dataframe[dataframe.index <= 2400].copy()

    # Bin LAF values by specified bin_size
    dataframe["LAF_bin"] = dataframe.index.to_series().apply(
        lambda x: int(x // bin_size) * bin_size
    )

    # Group by binned LAF and aggregate to averages
    dataframe = dataframe.groupby("LAF_bin").mean()
    dataframe.index.name = "LAF"

    return dataframe


def plot_heatmap(
    dataframe: pd.DataFrame,
    title="A title",
    max_temp=80,
    min_temp=0,
    well_top=0,
    save_html=False,
) -> HTML:
    """
    Plot a heatmap of temperature data with LAF on the y-axis and timestamps on the x-axis.
    Args:
        dataframe: DataFrame with LAF as index and datetime columns containing TMP values.
        title: Title to display at the top of the heatmap.
        max_temp: Maximum temperature value for the visual map color scale.
        min_temp: Minimum temperature value for the visual map color scale.
        well_top: Value subtracted from left Y-axis labels to compute the right (secondary) Y-axis labels.
        save_html: Whether to save the heatmap as an HTML file.
    Returns:
        Rendered notebook visualization of the heatmap.
    """
    # Prepare data for heatmap: [x_index, y_index, value]
    heatmap_data = []
    columns = dataframe.columns.tolist()
    laf_values = dataframe.index.tolist()
    for x_idx, col in enumerate(columns):
        for y_idx, laf in enumerate(laf_values):
            value = dataframe.loc[laf, col]
            if pd.notna(value):
                heatmap_data.append([x_idx, y_idx, round(value, 2)])

    # Format x-axis labels (datetime columns)
    x_labels = [
        col.strftime("%Y-%m-%d %H:%M") if hasattr(col, "strftime") else str(col)
        for col in columns
    ]
    y_labels = [str(laf) for laf in laf_values]
    secondary_y_labels = [str(round(float(laf) - well_top)) for laf in laf_values]

    heatmap = (
        HeatMap(init_opts=opts.InitOpts(width="1200px", height="800px"))
        .add_xaxis(x_labels)
        .add_yaxis(
            series_name="Temperature",
            yaxis_data=y_labels,
            value=heatmap_data,
            label_opts=opts.LabelOpts(is_show=False),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title=title, pos_left="center"),
            legend_opts=opts.LegendOpts(is_show=False),
            visualmap_opts=opts.VisualMapOpts(
                min_=min_temp,
                max_=max_temp,
                is_calculable=True,
                orient="horizontal",
                pos_left="center",
                pos_bottom="0px",
            ),
            xaxis_opts=opts.AxisOpts(
                type_="category",
                axislabel_opts=opts.LabelOpts(rotate=45, font_size=8),
            ),
            yaxis_opts=opts.AxisOpts(
                type_="category",
                is_inverse=True,
                axislabel_opts=opts.LabelOpts(font_size=8),
            ),
            tooltip_opts=opts.TooltipOpts(
                is_show=True,
                formatter=JsCode(
                    """
                    function (params) {
                        var v = params.value || params.data;
                        var z = (v && v.length >= 3) ? v[2] : v;
                        if (typeof z === 'number') return z.toFixed(2);
                        return z;
                    }
                    """
                ),
            ),
            datazoom_opts=[
                opts.DataZoomOpts(
                    type_="slider",
                    orient="horizontal",
                    range_start=0,
                    range_end=10,
                    pos_bottom="60px",
                ),
                opts.DataZoomOpts(
                    type_="slider", orient="vertical", range_start=0, range_end=100
                ),
            ],
        )
    )

    heatmap.options["grid"] = [
        {"left": "60px", "right": "100px", "top": "80px", "bottom": "170px"}
    ]

    # Override xAxis to prevent pyecharts from conflicting with yAxis override
    heatmap.options["xAxis"] = [
        {
            "type": "category",
            "data": x_labels,
            "axisLabel": {"rotate": 45, "fontSize": 8},
            # "name": "Measurement Timestamp",
            # "nameLocation": "middle",
            # "nameGap": 40,
            # "nameTextStyle": {"fontSize": 12},
        }
    ]

    # Override yAxis to inject primary (left) + secondary (right) axes
    heatmap.options["yAxis"] = [
        # Primary axis (left) — original LAF values
        {
            "type": "category",
            "data": y_labels,
            "inverse": True,
            "axisLabel": {"fontSize": 8},
            "position": "left",
            "name": "Length Along Fiber (m)",
            "nameLocation": "middle",
            "nameGap": 40,
            "nameRotate": 90,
            "nameTextStyle": {"fontSize": 12},
        },
        # Secondary axis (right) — LAF values offset by origin
        {
            "type": "category",
            "data": secondary_y_labels,
            "inverse": True,
            "axisLabel": {"fontSize": 8},
            "position": "right",
            "axisLine": {"show": True},
            "splitLine": {"show": False},
            "name": "Length along well (m)",
            "nameLocation": "middle",
            "nameGap": 10,
            "nameRotate": 90,
            "nameTextStyle": {"fontSize": 12},
        },
    ]

    if save_html:
        heatmap.render(f"{'_'.join(title.split())}.html")
    return heatmap.render_notebook()
