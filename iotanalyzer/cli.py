import argparse
import datetime
from models import Metric, Unit

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process IOT sensor data")
    parser.add_argument("input_file", type=str, help="The input file to process")
    parser.add_argument("output_file", type=str, help="The output file to write")
    
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
        help="Repeat to request multiple aggregates.",
    )
    
    args = parser.parse_args()

    print(args)
    
    
