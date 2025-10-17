from filters import Filter
from statistics import Statistic
from reader import recordings_from_csv
from models import Recording

def process_recordings(input_file: str, output_file: str, filters: list[Filter] = None, statistics: list[Statistic] = None) -> None:
    
    for stat in statistics or []:
        stat.begin_pass(is_second_pass=False)
    
    for record in recordings_from_csv(input_file):
        if not all(f(record) for f in filters or []):
            continue
        for stat in statistics or []:
            stat.consume(record)
    
    # for statistics that require a second pass
    second_pass_stats = [stat for stat in statistics or [] if stat.requires_second_pass]
    for stat in second_pass_stats:
        stat.begin_pass(is_second_pass=True)
    if second_pass_stats:
        for record in recordings_from_csv(input_file):
            if not all(f(record) for f in filters or []):
                continue
            for stat in second_pass_stats:
                stat.consume(record)

    for stat in statistics or []:
        print("Results for", stat.name, ":", stat.get_result())
