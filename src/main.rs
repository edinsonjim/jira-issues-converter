use std::{fs::File, sync::Arc};

use polars::{
    io::{SerReader, SerWriter},
    lazy::dsl::{col, concat_str, GetOutput},
    prelude::{CsvReadOptions, CsvWriter, IntoLazy},
    series::Series,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Decoder {
    input_key: String,
    output_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DecoderConfig {
    sprints: Vec<Decoder>,
    epics: Vec<Decoder>,
}

fn map_col_series(series: &Series, decoders: &Vec<Decoder>) -> Option<Series> {
    let new_series = series
        .iter()
        .map(|row| {
            let row_val = row.get_str().unwrap();
            decode_row_with(row_val, decoders)
        })
        .collect();

    Some(new_series)
}

fn decode_row_with(row_val: &str, decoders: &Vec<Decoder>) -> String {
    decoders
        .iter()
        .find(|&decoder| decoder.input_key.eq(row_val))
        .map(|decoder| decoder.output_key.clone())
        .unwrap_or_else(|| "Not Decoded".to_owned())
}

fn main() {
    let decoder_file = File::open("./decoder.yml").expect("failed to open decoder.yml");
    let decoder_settings: DecoderConfig =
        serde_yaml::from_reader(decoder_file).expect("failed to read decoder");
    let conf = Arc::new(decoder_settings);

    let df = CsvReadOptions::default()
        .try_into_reader_with_file_path(Some("./data/Jira 2024-06-12T11_32_40+0200.csv".into()))
        .unwrap()
        .finish()
        .unwrap();

    let mut transformed = df
        .clone()
        .lazy()
        .with_columns([
            // some sample
            (col("Sprint").map(
                {
                    let conf = Arc::clone(&conf);
                    move |series: Series| {
                        let decoder_config = &conf.sprints;
                        let new_series = map_col_series(&series, decoder_config);
                        Ok(new_series)
                    }
                },
                GetOutput::same_type(),
            ))
            .alias("Sprint Output"),
            (col("Custom field (Epic Link)")
                .map(
                    {
                        let conf = Arc::clone(&conf);
                        move |series| {
                            let decoder_config = &conf.epics;
                            let new_series = map_col_series(&series, decoder_config);
                            Ok(new_series)
                        }
                    },
                    GetOutput::same_type(),
                )
                .alias("Epic Link Output")),
            // concat issue key with summary
            concat_str([col("Issue key"), col("Summary")], " | ", true).alias("Summary Output"),
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
            col("Summary Output"),
            col("Sprint Output"),
            col("Epic Link Output"),
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
