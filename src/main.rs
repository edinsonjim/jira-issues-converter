use std::fs::File;

use env_logger::Env;
use polars::{
    frame::DataFrame,
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

fn read_decoder_config(path: &str) -> DecoderConfig {
    let file = File::open(path).expect("failed to open decoder.yml");
    serde_yaml::from_reader(file).expect("failed to read decoder")
}

fn write_csv_file(path: &str, df: &mut DataFrame) {
    let mut file = File::create(path).expect("could not create output file");
    CsvWriter::new(&mut file)
        .include_header(true)
        .with_separator(b',')
        .finish(df)
        .expect("could not write transformed file");
}

fn read_csv_file(path: &str) -> DataFrame {
    CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path.into()))
        .expect("failed to read CSV file")
        .finish()
        .expect("failed to parse CSV file")
}

fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    log::info!("loading decoder config");
    let decoder_conf = read_decoder_config("./decoder.yml");

    log::info!("reading csv file");
    let df = read_csv_file("./input.csv");

    log::info!("decoding csv file");
    let mut df_decoded = df
        .clone()
        .lazy()
        .with_columns([
            (col("Sprint").map(
                move |series: Series| Ok(map_col_series(&series, &decoder_conf.epics)),
                GetOutput::same_type(),
            ))
            .alias("Sprint Output"),
            (col("Custom field (Epic Link)")
                .map(
                    move |series| Ok(map_col_series(&series, &decoder_conf.sprints)),
                    GetOutput::same_type(),
                )
                .alias("Epic Link Output")),
            concat_str([col("Issue key"), col("Summary")], " - ", true).alias("Summary Output"),
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
        .expect("failed to collect data frame");

    log::info!("writing output file");
    write_csv_file("./output.csv", &mut df_decoded);
}
