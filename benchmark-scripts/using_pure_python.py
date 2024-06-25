from csv import reader
from collections import defaultdict
import time

from pathlib import Path

def process_temperatures(path_do_txt: Path):
    print("Starting file processing.")
    start_time = time.time()  # Tempo de início

    temperature_by_station = defaultdict(list)

    with open(path_do_txt, 'r', encoding="utf-8") as file:
        _reader = reader(file, delimiter=';')
        for row in _reader:
            station_name, temperatura = str(row[0]), float(row[1])
            temperature_by_station[station_name].append(temperatura)

    print("Data loaded, calculating statistics...")

    # dict to store results
    results = {}

    # statistics for each station
    for station, temperatures in temperature_by_station.items():
        min_temp = min(temperatures)
        mean_temp = sum(temperatures) / len(temperatures)
        max_temp = max(temperatures)
        results[station] = (min_temp, mean_temp, max_temp)

    print("Statistics completed. Sorting...")
    # sorting by station number
    sorted_results = dict(sorted(results.items()))

    # formatting results
    formatted_results = {station: f"{min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}" for station, (min_temp, mean_temp, max_temp) in sorted_results.items()}

    end_time = time.time()  # Tempo de término
    print(f"Processing completed {end_time - start_time:.2f} segundos.")

    return formatted_results

# Substitua "data/measurements10M.txt" pelo caminho correto do seu arquivo
if __name__ == "__main__":

    # 1M 0.38 segundos
    # 10M 3.96 segundos.
    path_do_txt: Path = Path("../data/measurements.txt")
    # 100M > 5 minutos.
    resultados = process_temperatures(path_do_txt)