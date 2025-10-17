import argparse
import datetime
from datetime import timezone
from typing import List

from iotanalyzer.filters import (
    Filter,
    device_filter,
    end_time_filter,
    metric_filter,
    site_filter,
    start_time_filter,
)
from iotanalyzer.models import Metric
from iotanalyzer.processor import process_recordings
from iotanalyzer.statistics import statistic_from_string

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process IOT sensor data")
    parser.add_argument("input_file", type=str, help="The input file to process")
    
    def parse_datetime(value: str) -> datetime.datetime:
        naive = datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return naive.replace(tzinfo=timezone.utc)

    parser.add_argument(
        "--start", 
        type=parse_datetime, 
        help="Only include recordings after this time in YYYY-MM-DD HH:MM:SS format"
    )
    parser.add_argument(
        "--end", 
        type=parse_datetime, 
        help="Only include recordings before this time in YYYY-MM-DD HH:MM:SS format"
    )
    parser.add_argument(
        "--site", 
        action="extend", 
        nargs="*", 
        type=str, 
        help="Only include recordings for these sites"
    )
    parser.add_argument(
        "--metric", 
        action="extend", 
        nargs="*", 
        type=Metric.from_string, 
        help="Only include recordings for these metrics"
    )
    parser.add_argument(
        "--device", 
        action="extend", 
        nargs="*", 
        type=str,
        help="Only include recordings for these devices"
    )

    parser.add_argument(
        "--statistic",
        action="extend",
        nargs="+",
        type=statistic_from_string,
        help=(
            "Repeat to request multiple aggregates. "
            "Format: name[:option=value,...]. "
            "Options: sort={value_asc,value_desc,device_site_metric}, k=<int>, name=<label>."
        ),
    )
    
    args = parser.parse_args()

    filters: List[Filter] = []
    if args.start:
        filters.append(start_time_filter(args.start))
    if args.end:
        filters.append(end_time_filter(args.end))
    if args.site:
        filters.append(site_filter(args.site))
    if args.metric:
        filters.append(metric_filter(args.metric))
    if args.device:
        filters.append(device_filter(args.device))

    process_recordings(
        input_file=args.input_file,
        filters=filters,
        statistics=args.statistic
    )
