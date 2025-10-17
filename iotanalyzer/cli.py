import argparse
import datetime
from iotanalyzer.models import Metric, Unit
from iotanalyzer.processor import process_recordings
from iotanalyzer.statistics import statistic_from_string

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process IOT sensor data")
    parser.add_argument("input_file", type=str, help="The input file to process")
    
    convert_to_datetime = lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    parser.add_argument(
        "--start", 
        type=convert_to_datetime, 
        help="Only include recordings after this time in YYYY-MM-DD HH:MM:SS format"
    )
    parser.add_argument(
        "--end", 
        type=convert_to_datetime, 
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
        type=Unit.from_string, 
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

    print(args)
    
    process_recordings(
        input_file=args.input_file,
        statistics=args.statistic
    )
