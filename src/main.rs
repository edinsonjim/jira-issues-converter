use std::fs::File;

use polars::{
    io::{SerReader, SerWriter},
    lazy::dsl::{col, concat_str, GetOutput},
    prelude::{CsvReadOptions, CsvWriter, IntoLazy},
};

fn main() {
    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some("./data/Jira 2024-06-10T16_46_41+0200.csv".into()))
        .unwrap()
        .finish()
        .unwrap();

    let mut transformed = df
        .clone()
        .lazy()
        .with_columns([
            // some sample
            (col("Sprint").map(
                |series| {
                    let new_series = series
                        .iter()
                        .map(|row| {
                            let val = row.get_str().unwrap();
                            let result = match val {
                                "Sprint-2024-05-S5" => "2024-05-S5",
                                _ => "NO_SPRINT",
                            };
                            result
                        })
                        .collect();

                    Ok(Some(new_series))
                },
                GetOutput::same_type(),
            ))
            .alias("Sprint Lynx"),
            // concat issue key with summary
            concat_str([col("Issue key"), col("Summary")], " | ", true).alias("Summary"),
        ])
        .select([
            col("Summary"),
            col("Issue key"),
            col("Issue Type"),
            col("Custom field (Story Points)"),
            col("Priority"),
            col("Custom field (Epic Link)"),
            col("Fix Version/s"),
            col("Description"),
            col("Sprint"),
            col("Sprint Lynx"),
        ])
        .collect()
        .expect("failed to collect by issue key");

    println!("{}", transformed);

    let mut file = File::create("./dist/output.csv").expect("could not create output file");
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(&mut transformed)
        .expect("could not write transformed file");
}
