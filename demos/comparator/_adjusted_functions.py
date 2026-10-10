import csv 
import json

def load(load_path, file_name):
    path = load_path / (file_name)
    
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))

def new_clean(raw_data):
    return [
        {"value": int(row["value"].strip())}
        for row in raw_data
        if row.get("value") is not None and row["value"].strip()
    ]

def old_process(cleaned_data):
    values = [row["value"] for row in cleaned_data]
    return {
        "count": len(values),
        "total": sum(values),
        "average": sum(values) / len(values),
    }

def report(processed_data):
    return {
        "title": "Pipeline Summary",
        "summary": processed_data
    }

def export(processed_data, export_path):
    with export_path.open("w", encoding="utf-8") as file:
        json.dump(processed_data, file, indent=2)
    return str(export_path)
