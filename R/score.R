#' Write scores
#'
#' @param predicted modeled predictions in Epinow2 pipeline format
#' @param report_date The vintage of the data to which the model was fit.
#' Currently only used to check that the vintage is not too
#' recent relative to the final data.
#' @param final Finalized, observed data. Usually the latest available vintage.
#' @param output Path to write scored estimates in parquet format
#' @return The output path of the file
#' @export
write_scores <- function(predicted, report_date, final, output) {
  final <- arrow::read_parquet(final) |>
    dplyr::filter(metric == "count_ed_visits", !is.na(geo_value)) |>
    dplyr::summarize(
      observed = sum(value),
      .by = c("reference_date", "report_date", "disease", "geo_value")
    ) |>
    dplyr::select(reference_date, disease, geo_value, observed)

  predicted <- arrow::read_parquet(predicted) |>
    # Subset to forecasted cases
    filter(`_variable` == "pp_nowcast_cases") |>
    dplyr::mutate(
      report_date = as.Date(report_date),
      # Last observed data point is day before report date
      horizon = as.integer(reference_date - report_date + 1)
    ) |>
    dplyr::filter(horizon >= -14)


  # Format for `{scoringutils}`
  forecast <- dplyr::inner_join(
    predicted,
    final,
    by = dplyr::join_by(reference_date, geo_value, disease)
  ) |>
    dplyr::rename(sample_id = `_draw`, predicted = value) |>
    scoringutils::as_forecast_sample(
      forecast_unit = c(
        "reference_date",
        "report_date",
        "horizon",
        "geo_value",
        "disease"
      )
    )

  scores <- scoringutils::score(forecast)

  arrow::write_parquet(scores, output)
  return(output)
}
