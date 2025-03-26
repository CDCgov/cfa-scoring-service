suppressMessages(library(dplyr))
suppressMessages(library(arrow))
suppressMessages(library(scoringutils))
suppressMessages(library(optparse, quietly = TRUE))

# Input parameters
option_list = list(
  make_option(
    c("-p", "--predicted"),
    help = "Modeled predictions in EpiNow2 pipeline format",
    type = "character"
  ),
  make_option(
    c("-r", "--report_date"),
    help = "The vintage of the data to which the model was fit.
    Currently only used to check that the vintage is not too
    recent relative to the final data.
    ",
    type = "character"
  ),
  make_option(
    c("-f", "--final"),
    help = "Finalized, observed data. Usually the latest available vintage.",
    type = "character"
  ),
  make_option(
    c("-o", "--output"),
    help = "Path to write scored estimates in parquet format",
    type = "character"
  )
)
opt_parser <- OptionParser(option_list = option_list)
opt <- parse_args(opt_parser)

final <- read_parquet(opt$final) |>
  filter(metric == "count_ed_visits", !is.na(geo_value)) |>
  summarize(
    observed = sum(value),
    .by = c("reference_date", "report_date", "disease", "geo_value")
  ) |>
  select(reference_date, disease, geo_value, observed)

predicted <- read_parquet(opt$predicted) |>
  # Subset to forecasted cases
  filter(`_variable` == "pp_nowcast_cases") |>
  mutate(
    report_date = as.Date(opt$report_date),
    # Last observed data point is day before report date
    horizon = as.integer(reference_date - report_date + 1)
  ) |>
  filter(horizon >= -14)


# Format for `{scoringutils}`
forecast <- inner_join(
  predicted,
  final,
  by = join_by(reference_date, geo_value, disease)
) |>
  rename(sample_id = `_draw`, predicted = value) |>
  as_forecast_sample(
    forecast_unit = c(
      "reference_date",
      "report_date",
      "horizon",
      "geo_value",
      "disease"
    )
  )

scores <- score(forecast)

write_parquet(scores, opt$output)
