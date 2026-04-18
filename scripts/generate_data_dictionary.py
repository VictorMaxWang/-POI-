from __future__ import annotations

from pipeline_common import DOCS_DIR, iter_schema_rows, write_csv_rows


def main() -> None:
    fieldnames = ["table_name", "field_name", "field_type", "required", "description"]
    write_csv_rows(DOCS_DIR / "data_dictionary.csv", fieldnames, iter_schema_rows())
    print("data_dictionary generated")


if __name__ == "__main__":
    main()
